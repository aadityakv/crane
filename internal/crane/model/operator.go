package model

import (
	"errors"
	"math"
	"strconv"
)

// SourceTuple returns the sole deterministic tuple for a source sequence.
func SourceTuple(topology ValidatedTopology, task TaskID, sequence uint64) (Tuple, bool, error) {
	if err := task.Validate(); err != nil {
		return Tuple{}, false, err
	}
	if sequence == 0 {
		return Tuple{}, false, errors.New("source sequence must be nonzero")
	}
	stage, ok := topology.byStage[task.StageID]
	if !ok || stage.Role != StageSource {
		return Tuple{}, false, errors.New("task does not identify the source stage")
	}
	if task.Partition >= stage.Parallelism {
		return Tuple{}, false, errors.New("source partition is outside stage parallelism")
	}
	eof := topology.eofs[stage.StageID][task.Partition]
	if sequence > eof {
		return Tuple{}, false, nil
	}
	if stage.Operator.Name == "lines" {
		return linesTuple(stage.Operator, stage.Parallelism, task.Partition, sequence)
	}
	start, _, err := rangeSettings(stage.Operator)
	if err != nil {
		return Tuple{}, false, err
	}
	base := sequence - 1
	if base > (math.MaxUint64-uint64(task.Partition))/uint64(stage.Parallelism) {
		return Tuple{}, false, errors.New("source ordinal overflow")
	}
	ordinal := base*uint64(stage.Parallelism) + uint64(task.Partition)
	value, ok := addInt64Uint64(start, ordinal)
	if !ok {
		return Tuple{}, false, errors.New("source value overflow")
	}
	return Tuple{Fields: []Field{{Name: "value", Value: Value{Type: ValueInt64, Int64: value}}}}, true, nil
}

// SourceEOF returns the exact last valid source sequence for a source task.
func SourceEOF(topology ValidatedTopology, task TaskID) (uint64, error) {
	if err := task.Validate(); err != nil {
		return 0, err
	}
	stage, ok := topology.byStage[task.StageID]
	if !ok || stage.Role != StageSource {
		return 0, errors.New("task does not identify the source stage")
	}
	if task.Partition >= stage.Parallelism {
		return 0, errors.New("source partition is outside stage parallelism")
	}
	return topology.eofs[stage.StageID][task.Partition], nil
}

// ExecuteOperator executes one deterministic transform or sink operator.
func ExecuteOperator(operator OperatorSpec, input Tuple) ([]Tuple, error) {
	descriptor, err := validateOperatorSpec(operator)
	if err != nil {
		return nil, err
	}
	if descriptor.Role == OperatorRoleSource {
		return nil, errors.New("source operators execute through SourceTuple")
	}
	if err := input.Validate(); err != nil {
		return nil, err
	}
	switch operator.Name {
	case "collect":
		return []Tuple{cloneTuple(input)}, nil
	case "split_words":
		return splitWords(input)
	case "min_length":
		return minLength(operator, input)
	}
	value, err := exactIntValue(input)
	if err != nil {
		return nil, err
	}
	switch operator.Name {
	case "multiply":
		factor, err := strconv.ParseInt(operator.Settings[0].Value, 10, 64)
		if err != nil {
			return nil, err
		}
		product, ok := multiplyInt64(value, factor)
		if !ok {
			return nil, errors.New("multiply overflow")
		}
		return []Tuple{intTupleValue(product)}, nil
	case "even":
		if value%2 != 0 {
			return []Tuple{}, nil
		}
		return []Tuple{cloneTuple(input)}, nil
	case "less_than":
		threshold, err := strconv.ParseInt(operator.Settings[0].Value, 10, 64)
		if err != nil {
			return nil, err
		}
		if value >= threshold {
			return []Tuple{}, nil
		}
		return []Tuple{cloneTuple(input)}, nil
	default:
		return nil, errors.New("unsupported operator")
	}
}

func rangeEOFs(operator OperatorSpec, parallelism uint16) ([]uint64, error) {
	result := make([]uint64, parallelism)
	for partition := uint16(0); partition < parallelism; partition++ {
		eof, err := sourceEOF(operator, parallelism, partition)
		if err != nil {
			return nil, err
		}
		result[partition] = eof
	}
	return result, nil
}

func sourceEOF(operator OperatorSpec, parallelism, partition uint16) (uint64, error) {
	if parallelism == 0 || partition >= parallelism {
		return 0, errors.New("invalid source partition")
	}
	var length uint64
	if operator.Name == "lines" {
		chunks, err := linesSettings(operator)
		if err != nil {
			return 0, err
		}
		length = uint64(len(chunks))
	} else {
		start, end, err := rangeSettings(operator)
		if err != nil {
			return 0, err
		}
		length = uint64(end) - uint64(start)
	}
	if length <= uint64(partition) {
		return 0, nil
	}
	eof := (length-1-uint64(partition))/uint64(parallelism) + 1
	if eof > LimitsV1().MaxSourceSequences {
		return 0, errors.New("source sequence limit exceeded")
	}
	return eof, nil
}

func rangeSettings(operator OperatorSpec) (int64, int64, error) {
	descriptor, err := validateOperatorSpec(operator)
	if err != nil || descriptor.Role != OperatorRoleSource || operator.Name != "range" {
		return 0, 0, errors.New("invalid range operator")
	}
	end, err := strconv.ParseInt(operator.Settings[0].Value, 10, 64)
	if err != nil {
		return 0, 0, err
	}
	start, err := strconv.ParseInt(operator.Settings[1].Value, 10, 64)
	if err != nil {
		return 0, 0, err
	}
	if start > end {
		return 0, 0, errors.New("range start exceeds end")
	}
	length := uint64(end) - uint64(start)
	if length > LimitsV1().MaxSourceSequences*256 {
		return 0, 0, errors.New("range exceeds maximum source capacity")
	}
	return start, end, nil
}

func exactIntValue(tuple Tuple) (int64, error) {
	if len(tuple.Fields) != 1 || tuple.Fields[0].Name != "value" || tuple.Fields[0].Value.Type != ValueInt64 {
		return 0, errors.New("operator requires exactly one int64 value field")
	}
	return tuple.Fields[0].Value.Int64, nil
}

func intTupleValue(value int64) Tuple {
	return Tuple{Fields: []Field{{Name: "value", Value: Value{Type: ValueInt64, Int64: value}}}}
}

func cloneTuple(tuple Tuple) Tuple {
	clone := Tuple{Fields: append([]Field(nil), tuple.Fields...)}
	for index := range clone.Fields {
		clone.Fields[index].Value.Bytes = append([]byte(nil), tuple.Fields[index].Value.Bytes...)
	}
	return clone
}

func addInt64Uint64(value int64, add uint64) (int64, bool) {
	if add > uint64(math.MaxInt64)-uint64(value) { // unsigned subtraction works for negative value.
		return 0, false
	}
	return int64(uint64(value) + add), true
}

func multiplyInt64(left, right int64) (int64, bool) {
	if left == 0 || right == 0 {
		return 0, true
	}
	if left == math.MinInt64 && right == -1 || right == math.MinInt64 && left == -1 {
		return 0, false
	}
	product := left * right
	return product, product/right == left
}
