package kgo

import (
	"slices"
	"testing"
)

// TestBuildAckRangesInterleavedGaps covers a read_uncommitted partition
// holding normal, committed, and aborted records: offsets 2 and 4 are
// transaction control records, so they are acquired but become internal
// gap acks. The broker requires batches in offset order, so the gaps must
// be interleaved with the user acks rather than appended after them.
func TestBuildAckRangesInterleavedGaps(t *testing.T) {
	src := new(source)
	slab := &shareAckSlab{ackSource: src, sessionEpoch: 1}
	gap := func(o int64) shareAckRange {
		return shareAckRange{firstOffset: o, lastOffset: o, source: src, sessionEpoch: 1}
	}
	entry := func(o int64, s AckStatus) *shareAckState {
		e := &shareAckState{offset: o, slab: slab}
		e.status.Store(int32(s))
		return e
	}

	for _, tc := range []struct {
		name    string
		entries []*shareAckState
		gaps    []shareAckRange
		want    [][3]int64 // first, last, type
	}{
		{
			name:    "control records between user acks",
			entries: []*shareAckState{entry(0, AckAccept), entry(1, AckAccept), entry(3, AckAccept)},
			gaps:    []shareAckRange{gap(2), gap(4)},
			want:    [][3]int64{{0, 1, 1}, {2, 2, 0}, {3, 3, 1}, {4, 4, 0}},
		},
		{
			name:    "unsorted inputs",
			entries: []*shareAckState{entry(3, AckAccept), entry(0, AckAccept), entry(1, AckAccept)},
			gaps:    []shareAckRange{gap(4), gap(2)},
			want:    [][3]int64{{0, 1, 1}, {2, 2, 0}, {3, 3, 1}, {4, 4, 0}},
		},
		{
			name:    "leading gap",
			entries: []*shareAckState{entry(5, AckRelease)},
			gaps:    []shareAckRange{{firstOffset: 0, lastOffset: 4, source: src, sessionEpoch: 1}},
			want:    [][3]int64{{0, 4, 0}, {5, 5, 2}},
		},
		{
			name: "gaps only",
			gaps: []shareAckRange{gap(3), gap(2)},
			want: [][3]int64{{2, 3, 0}},
		},
		{
			name:    "undecided entry skipped",
			entries: []*shareAckState{entry(0, AckAccept), entry(1, 0), entry(3, AckAccept)},
			gaps:    []shareAckRange{gap(2)},
			want:    [][3]int64{{0, 0, 1}, {2, 2, 0}, {3, 3, 1}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ranges, _ := buildAckRanges(tc.entries, slices.Clone(tc.gaps))
			var got [][3]int64
			prevEnd := int64(-1)
			for _, r := range ranges {
				if r.firstOffset <= prevEnd {
					t.Errorf("range %d-%d starts before previous end %d", r.firstOffset, r.lastOffset, prevEnd)
				}
				prevEnd = r.lastOffset
				got = append(got, [3]int64{r.firstOffset, r.lastOffset, int64(r.ackType)})
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}
