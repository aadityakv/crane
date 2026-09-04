package control

import (
	"errors"
	"fmt"
	"sort"

	"github.com/aadityakv/crane/internal/config"
	"github.com/aadityakv/crane/internal/crane/protocol"
)

// ErrInvalidVoterEndpoint reports a configured Raft voter endpoint whose
// canonical +4 control endpoint cannot be derived or validated.
var ErrInvalidVoterEndpoint = errors.New("invalid Crane voter control endpoint")

// deriveVoterControlEndpoints derives the complete sorted unique canonical +4
// control endpoint set of the configured static voters using the checked
// +6-to-+4 helper, and proves the complete redirect encodes canonically before
// any request is served.
func deriveVoterControlEndpoints(voters []config.RaftVoter) ([]string, error) {
	if len(voters) != 3 && len(voters) != 5 {
		return nil, fmt.Errorf("%w: %d voters", ErrInvalidVoterEndpoint, len(voters))
	}
	endpoints := make([]string, 0, len(voters))
	for _, voter := range voters {
		endpoint, err := deriveVoterControlEndpoint(voter)
		if err != nil {
			return nil, err
		}
		endpoints = append(endpoints, endpoint)
	}
	sort.Strings(endpoints)
	for index := 1; index < len(endpoints); index++ {
		if endpoints[index-1] >= endpoints[index] {
			return nil, fmt.Errorf("%w: duplicate derived endpoint %q", ErrInvalidVoterEndpoint, endpoints[index])
		}
	}
	if _, err := protocol.MarshalControlMessage(protocol.LeaderRedirect{Endpoints: append([]string(nil), endpoints...)}); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidVoterEndpoint, err)
	}
	return endpoints, nil
}

// deriveVoterControlEndpoint derives one voter's canonical +4 endpoint from
// its validated static +6 Raft endpoint with checked port arithmetic.
func deriveVoterControlEndpoint(voter config.RaftVoter) (string, error) {
	raftEndpoint, err := config.ParseRoutableEndpoint(voter.Endpoint)
	if err != nil {
		return "", fmt.Errorf("%w: voter %d: %v", ErrInvalidVoterEndpoint, voter.NodeID, err)
	}
	controlEndpoint, err := config.CraneControlEndpointFromRaft(raftEndpoint)
	if err != nil {
		return "", fmt.Errorf("%w: voter %d: %v", ErrInvalidVoterEndpoint, voter.NodeID, err)
	}
	return controlEndpoint.String(), nil
}

// staticRedirect returns an owned copy of the complete static voter redirect.
func (service *Service) staticRedirect() protocol.LeaderRedirect {
	return protocol.LeaderRedirect{Endpoints: append([]string(nil), service.voterEndpoints...)}
}

// leaderRedirect returns the derived +4 endpoint of one checked known leader,
// or the complete static voter redirect when the hint is zero, local, or not a
// configured voter.
func (service *Service) leaderRedirect(hint uint16) protocol.LeaderRedirect {
	if hint != 0 && hint != service.configuration.NodeID {
		if voter, ok := service.configuration.RaftVoterByID(hint); ok {
			if endpoint, err := deriveVoterControlEndpoint(voter); err == nil {
				return protocol.LeaderRedirect{Endpoints: []string{endpoint}}
			}
		}
	}
	return service.staticRedirect()
}
