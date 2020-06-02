package kgo

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestGroupETL tests:
//
// - producing a lot of messages to a single topic, ensuring that all messages
// are produced in order.
//
// - a chain of consumer groups with members joining and leaving
//
// - that we never doubly consume records, and that all records are consumed in
// order
//
// - all balancers
func TestGroupETL(t *testing.T) {
	t.Parallel()

	topic1, topic1Cleanup := tmpTopic(t)
	defer topic1Cleanup()

	errs := make(chan error)
	body := []byte(randsha()) // a small body so we do not flood RAM

	////////////////////
	// PRODUCER START //
	////////////////////

	go func() {
		cl, _ := NewClient(WithLogger(BasicLogger(testLogLevel, nil)))
		defer cl.Close()

		var offsetsMu sync.Mutex
		offsets := make(map[int32]int64)

		defer func() {
			if err := cl.Flush(context.Background()); err != nil {
				errs <- fmt.Errorf("unable to flush: %v", err)
			}
		}()
		for i := 0; i < testRecordLimit; i++ {
			myKey := []byte(strconv.Itoa(i))
			cl.Produce(
				context.Background(),
				&Record{
					Topic: topic1,
					Key:   myKey,
					Value: body,
				},
				func(r *Record, err error) {
					if err != nil {
						errs <- fmt.Errorf("unexpected produce err: %v", err)
					}
					if !bytes.Equal(r.Key, myKey) {
						errs <- fmt.Errorf("unexpected out of order key; got %s != exp %v", r.Key, myKey)
					}

					// ensure the offsets for this partition are contiguous
					offsetsMu.Lock()
					current, ok := offsets[r.Partition]
					if ok && r.Offset != current+1 {
						errs <- fmt.Errorf("partition produced offsets out of order, got %d != exp %d", r.Offset, current+1)
					} else if !ok && r.Offset != 0 {
						errs <- fmt.Errorf("expected first produced record to partition to have offset 0, got %d", r.Offset)
					}
					offsets[r.Partition] = r.Offset
					offsetsMu.Unlock()
				},
			)
		}

	}()

	////////////////////////////
	// CONSUMER CHAINING TEST //
	////////////////////////////

	for _, tc := range []struct {
		name     string
		balancer GroupBalancer
	}{
		{"roundrobin", RoundRobinBalancer()},
		{"range", RangeBalancer()},
		{"sticky", StickyBalancer()},
		{"cooperative-sticky", CooperativeStickyBalancer()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testChainETL(
				t,
				topic1,
				body,
				errs,
				false,
				tc.balancer,
			)
		})
	}
}

func (c *testConsumer) goGroupETL(etlsBeforeQuit int) {
	c.wg.Add(1)
	go c.etl(etlsBeforeQuit)
}

func (c *testConsumer) etl(etlsBeforeQuit int) {
	defer c.wg.Done()
	cl, _ := NewClient(WithLogger(BasicLogger(testLogLevel, nil)))
	defer cl.Close()

	cl.AssignGroup(c.group,
		GroupTopics(c.consumeFrom),
		Balancers(c.balancer))

	netls := 0 // for if etlsBeforeQuit is non-negative

	defer func() {
		if err := cl.Flush(context.Background()); err != nil {
			c.errCh <- fmt.Errorf("unable to flush: %v", err)
		}
	}()

	for {
		// We poll with a short timeout so that we do not hang waiting
		// at the end if another consumer hit the limit.
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		fetches := cl.PollFetches(ctx)
		cancel()
		if len(fetches) == 0 {
			if consumed := atomic.LoadUint64(&c.consumed); consumed == testRecordLimit {
				return
			} else if consumed > testRecordLimit {
				panic("invalid: consumed too much")
			}
		}

		if fetchErrs := fetches.Errors(); len(fetchErrs) > 0 {
			c.errCh <- fmt.Errorf("poll got unexpected errs: %v", fetchErrs)
		}

		if etlsBeforeQuit >= 0 && netls >= etlsBeforeQuit {
			return
		}
		netls++

		for iter := fetches.RecordIter(); !iter.Done(); {
			r := iter.Next()
			keyNum, err := strconv.Atoi(string(r.Key))
			if err != nil {
				c.errCh <- err
			}
			if !bytes.Equal(r.Value, c.expBody) {
				c.errCh <- fmt.Errorf("body not what was expected")
			}

			c.mu.Lock()
			// check dup
			if _, exists := c.partOffsets[partOffset{r.Partition, r.Offset}]; exists {
				c.errCh <- fmt.Errorf("saw double offset p%do%d", r.Partition, r.Offset)
			}
			c.partOffsets[partOffset{r.Partition, r.Offset}] = struct{}{}

			// check ordering
			offsetOrdering := c.part2offsets[r.Partition]
			if len(offsetOrdering) > 0 && r.Offset < offsetOrdering[len(offsetOrdering)-1]+1 {
				c.errCh <- fmt.Errorf("part %d last offset %d; this offset %d; expected increasing", r.Partition, offsetOrdering[len(offsetOrdering)-1], r.Offset)
			}
			c.part2offsets[r.Partition] = append(offsetOrdering, r.Offset)

			// save key for later for all-keys-consumed validation
			c.part2key[r.Partition] = append(c.part2key[r.Partition], keyNum)
			c.mu.Unlock()

			atomic.AddUint64(&c.consumed, 1)

			cl.Produce(
				context.Background(),
				&Record{
					Topic: c.produceTo,
					Key:   r.Key,
					Value: r.Value,
				},
				func(_ *Record, err error) {
					if err != nil {
						c.errCh <- fmt.Errorf("unexpected etl produce err: %v", err)
					}
				},
			)
		}

	}

}
