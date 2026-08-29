package swim

import (
	"sort"

	"github.com/aaditya/cs425mp3/internal/random"
)

// probeSelector retains one shuffled cycle of eligible peer identities. The
// engine supplies its owner-confined membership snapshot on every selection so
// peers that became ineligible are skipped before use.
type probeSelector struct {
	source random.Source
	order  []uint16
	index  int
}

func (s *probeSelector) next(members []Member, selfID uint16) (Member, bool) {
	current := make(map[uint16]Member, len(members))
	for _, member := range members {
		current[member.NodeID] = member
	}

	for {
		if s.index >= len(s.order) {
			s.order = s.order[:0]
			for _, member := range members {
				if member.NodeID != selfID && member.Status == Alive {
					s.order = append(s.order, member.NodeID)
				}
			}
			if len(s.order) == 0 {
				s.index = 0
				return Member{}, false
			}
			sort.Slice(s.order, func(i, j int) bool { return s.order[i] < s.order[j] })
			s.source.Shuffle(len(s.order), func(i, j int) {
				s.order[i], s.order[j] = s.order[j], s.order[i]
			})
			s.index = 0
		}

		nodeID := s.order[s.index]
		s.index++
		member, exists := current[nodeID]
		if exists && member.NodeID != selfID && member.Status == Alive {
			return member, true
		}
	}
}
