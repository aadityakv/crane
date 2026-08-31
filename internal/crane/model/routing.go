package model

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
)

const routeDomain = "cs425/crane/route/v1\x00"

// Route returns canonical destination partitions for one validated edge.
func Route(topology ValidatedTopology, edge EdgeSpec, tupleID TupleID, tuple Tuple) ([]uint16, error) {
	if err := tupleID.Validate(); err != nil {
		return nil, err
	}
	if err := tuple.Validate(); err != nil {
		return nil, err
	}
	found := false
	for _, candidate := range topology.spec.Edges {
		if candidate == edge {
			found = true
			break
		}
	}
	if !found {
		return nil, errors.New("edge is not part of validated topology")
	}
	destination, ok := topology.byStage[edge.DestinationStageID]
	if !ok || destination.Parallelism == 0 {
		return nil, errors.New("invalid edge destination")
	}
	switch edge.Routing {
	case RoutingBroadcast:
		partitions := make([]uint16, destination.Parallelism)
		for index := range partitions {
			partitions[index] = uint16(index)
		}
		return partitions, nil
	case RoutingShuffle:
		encoded := append([]byte(routeDomain), tupleID.JobID[:]...)
		encoded = appendUint16(encoded, edge.EdgeID)
		encoded = appendTupleID(encoded, tupleID)
		digest := sha256.Sum256(encoded)
		return []uint16{uint16(binary.BigEndian.Uint64(digest[:8]) % uint64(destination.Parallelism))}, nil
	case RoutingFieldHash:
		var field *Field
		for index := range tuple.Fields {
			if tuple.Fields[index].Name == edge.Field {
				field = &tuple.Fields[index]
				break
			}
		}
		if field == nil {
			return nil, errors.New("routing field is missing")
		}
		encoded, err := canonicalFieldBytes(*field)
		if err != nil {
			return nil, err
		}
		digest := sha256.Sum256(encoded)
		return []uint16{uint16(binary.BigEndian.Uint64(digest[:8]) % uint64(destination.Parallelism))}, nil
	default:
		return nil, errors.New("unknown routing mode")
	}
}

func canonicalFieldBytes(field Field) ([]byte, error) {
	if err := (Tuple{Fields: []Field{field}}).Validate(); err != nil {
		return nil, err
	}
	w := newCheckedWriter(32)
	if err := w.string(field.Name); err != nil {
		return nil, err
	}
	_ = w.byte(byte(field.Value.Type))
	switch field.Value.Type {
	case ValueInt64:
		_ = w.uint64(uint64(field.Value.Int64))
	case ValueString:
		_ = w.string(field.Value.String)
	case ValueBytes:
		_ = w.bytes(field.Value.Bytes)
	}
	return w.ownedBytes(), nil
}

func compareDigest(left, right [32]byte) int { return bytes.Compare(left[:], right[:]) }
