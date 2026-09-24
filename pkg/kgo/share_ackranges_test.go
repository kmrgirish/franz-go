package kgo

import (
	"slices"
	"testing"
)

// TestShareBuildAckRangesOffsetOrder is the regression test for gap acks
// being appended after every user ack.
//
// A read_uncommitted share partition holding a normal record, a committed
// transaction, and an aborted transaction has data at offsets 0, 1, and 3
// and transaction markers at 2 and 4. The broker acquires 0-4, but only
// 0, 1, and 3 are returned to the user; processSharePartition queues gap
// acks for 2 and 4. If those gaps are drained alongside the user's acks,
// buildAckRanges previously emitted [0-1 accept] [3 accept] [2 gap]
// [4 gap]. Kafka rejects a partition whose ack batches are not in offset
// order (KafkaApis.validateAcknowledgementBatches) with INVALID_REQUEST,
// so the user's acks failed. Gaps must be interleaved with user acks by
// offset.
func TestShareBuildAckRangesOffsetOrder(t *testing.T) {
	t.Parallel()

	s := new(source)
	slab := &shareAckSlab{ackSource: s, sessionEpoch: 1}
	ack := func(offset int64, status AckStatus) *shareAckState {
		e := &shareAckState{offset: offset, slab: slab}
		e.status.Store(int32(status))
		return e
	}
	rng := func(first, last int64, ackType int8) shareAckRange {
		return shareAckRange{firstOffset: first, lastOffset: last, source: s, sessionEpoch: 1, ackType: ackType}
	}
	gap := func(first, last int64) shareAckRange { return rng(first, last, 0) }

	var (
		accept  = int8(AckAccept)
		release = int8(AckRelease)
	)

	tests := []struct {
		name    string
		entries []*shareAckState
		gaps    []shareAckRange
		exp     []shareAckRange
	}{
		{
			name:    "transaction markers between user acks",
			entries: []*shareAckState{ack(0, AckAccept), ack(1, AckAccept), ack(3, AckAccept)},
			gaps:    []shareAckRange{gap(2, 2), gap(4, 4)},
			exp:     []shareAckRange{rng(0, 1, accept), gap(2, 2), rng(3, 3, accept), gap(4, 4)},
		},
		{
			name:    "unsorted entries and gaps",
			entries: []*shareAckState{ack(3, AckAccept), ack(0, AckAccept), ack(1, AckAccept)},
			gaps:    []shareAckRange{gap(4, 4), gap(2, 2)},
			exp:     []shareAckRange{rng(0, 1, accept), gap(2, 2), rng(3, 3, accept), gap(4, 4)},
		},
		{
			name:    "gap before every user ack",
			entries: []*shareAckState{ack(5, AckRelease)},
			gaps:    []shareAckRange{gap(0, 4)},
			exp:     []shareAckRange{gap(0, 4), rng(5, 5, release)},
		},
		{
			name: "only gaps",
			gaps: []shareAckRange{gap(3, 3), gap(2, 2)},
			exp:  []shareAckRange{gap(2, 3)},
		},
		{
			name:    "undecided entry is skipped",
			entries: []*shareAckState{ack(0, AckAccept), ack(1, 0), ack(3, AckAccept)},
			gaps:    []shareAckRange{gap(2, 2)},
			exp:     []shareAckRange{rng(0, 0, accept), gap(2, 2), rng(3, 3, accept)},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ranges, _ := buildAckRanges(test.entries, slices.Clone(test.gaps))
			if !slices.Equal(ranges, test.exp) {
				t.Fatalf("expected %v, got %v", fmtAckRanges(test.exp), fmtAckRanges(ranges))
			}
			// Mirror the broker's validation: each batch must start
			// after the previous batch ended.
			prevEnd := int64(-1)
			for _, r := range ranges {
				if r.firstOffset <= prevEnd {
					t.Fatalf("range %d-%d starts at or before previous range end %d", r.firstOffset, r.lastOffset, prevEnd)
				}
				prevEnd = r.lastOffset
			}
		})
	}
}

func fmtAckRanges(ranges []shareAckRange) [][3]int64 {
	out := make([][3]int64, 0, len(ranges))
	for _, r := range ranges {
		out = append(out, [3]int64{r.firstOffset, r.lastOffset, int64(r.ackType)})
	}
	return out
}
