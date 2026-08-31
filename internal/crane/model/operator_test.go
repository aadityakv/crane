package model

import (
	"math"
	"testing"
)

func intTuple(value int64) Tuple {
	return Tuple{Fields: []Field{{Name: "value", Value: Value{Type: ValueInt64, Int64: value}}}}
}

func TestOperatorRangeSourceTupleAndEOFParity(t *testing.T) {
	v := requireValidTopology(t, testTopology())
	job := JobID{1}
	task0 := TaskID{JobID: job, StageID: 1, Partition: 0}
	task1 := TaskID{JobID: job, StageID: 1, Partition: 1}
	for _, tc := range []struct {
		task TaskID
		seq  uint64
		want int64
	}{
		{task0, 1, 1}, {task0, 2, 3}, {task0, 3, 5},
		{task1, 1, 2}, {task1, 2, 4}, {task1, 3, 6},
	} {
		got, ok, err := SourceTuple(v, tc.task, tc.seq)
		if err != nil || !ok || got.Fields[0].Value.Int64 != tc.want {
			t.Fatalf("SourceTuple(%d,%d) = %#v,%v,%v; want %d", tc.task.Partition, tc.seq, got, ok, err, tc.want)
		}
	}
	for _, task := range []TaskID{task0, task1} {
		eof, err := SourceEOF(v, task)
		if err != nil || eof != 3 {
			t.Fatalf("SourceEOF = %d,%v", eof, err)
		}
		got, ok, err := SourceTuple(v, task, eof+1)
		if err != nil || ok || len(got.Fields) != 0 {
			t.Fatalf("after EOF = %#v,%v,%v", got, ok, err)
		}
	}
	if _, _, err := SourceTuple(v, task0, 0); err == nil {
		t.Fatal("sequence zero accepted")
	}
	if _, _, err := SourceTuple(v, TaskID{JobID: job, StageID: 2}, 1); err == nil {
		t.Fatal("non-source accepted")
	}
	if _, _, err := SourceTuple(v, TaskID{JobID: job, StageID: 1, Partition: 2}, 1); err == nil {
		t.Fatal("invalid partition accepted")
	}
}

func TestOperatorRangeEmptyAndOverflowValidation(t *testing.T) {
	spec := testTopology()
	spec.Stages[0].Operator.Settings[0].Value = "5"
	spec.Stages[0].Operator.Settings[1].Value = "5"
	v := requireValidTopology(t, spec)
	eof, err := SourceEOF(v, TaskID{JobID: JobID{1}, StageID: 1})
	if err != nil || eof != 0 {
		t.Fatalf("empty range EOF = %d,%v", eof, err)
	}

	for _, values := range [][2]string{{"0", "x"}, {"0", "1"}, {"9223372036854775807", "-9223372036854775808"}} {
		spec = testTopology()
		spec.Stages[0].Operator.Settings[0].Value = values[0]
		spec.Stages[0].Operator.Settings[1].Value = values[1]
		if _, err := ValidateTopology(spec); err == nil {
			t.Fatalf("invalid range %q accepted", values)
		}
	}
	spec = testTopology()
	spec.Stages[0].Operator.Settings[1].Value = "+1"
	if _, err := ValidateTopology(spec); err == nil {
		t.Fatal("non-canonical signed integer spelling accepted")
	}
}

func TestOperatorTransformsExactSchemas(t *testing.T) {
	input := intTuple(6)
	out, err := ExecuteOperator(OperatorSpec{Name: "multiply", Version: 1, Settings: []Setting{{Key: "factor", Value: "7"}}}, input)
	if err != nil || len(out) != 1 || out[0].Fields[0].Value.Int64 != 42 {
		t.Fatalf("multiply = %#v,%v", out, err)
	}
	if _, err := ExecuteOperator(OperatorSpec{Name: "multiply", Version: 1, Settings: []Setting{{Key: "factor", Value: "2"}}}, intTuple(math.MaxInt64)); err == nil {
		t.Fatal("multiply overflow accepted")
	}
	for _, value := range []int64{2, 3} {
		out, err = ExecuteOperator(OperatorSpec{Name: "even", Version: 1}, intTuple(value))
		if err != nil || len(out) != map[bool]int{true: 1, false: 0}[value%2 == 0] {
			t.Fatalf("even(%d) = %#v,%v", value, out, err)
		}
	}
	out, err = ExecuteOperator(OperatorSpec{Name: "less_than", Version: 1, Settings: []Setting{{Key: "threshold", Value: "7"}}}, input)
	if err != nil || len(out) != 1 {
		t.Fatalf("less_than = %#v,%v", out, err)
	}
	out, err = ExecuteOperator(OperatorSpec{Name: "collect", Version: 1}, input)
	if err != nil || len(out) != 1 {
		t.Fatalf("collect = %#v,%v", out, err)
	}
	gotBytes, _ := MarshalTuple(out[0])
	wantBytes, _ := MarshalTuple(input)
	if string(gotBytes) != string(wantBytes) {
		t.Fatal("collect changed canonical tuple bytes")
	}
	structured := Tuple{Fields: []Field{
		{Name: "label", Value: Value{Type: ValueString, String: "eleven"}},
		{Name: "value", Value: Value{Type: ValueInt64, Int64: 11}},
	}}
	out, err = ExecuteOperator(OperatorSpec{Name: "collect", Version: 1}, structured)
	if err != nil || len(out) != 1 {
		t.Fatalf("collect rejected a valid complete canonical tuple: %#v,%v", out, err)
	}
	gotBytes, _ = MarshalTuple(out[0])
	wantBytes, _ = MarshalTuple(structured)
	if string(gotBytes) != string(wantBytes) {
		t.Fatal("collect did not preserve the complete canonical tuple")
	}

	invalid := []OperatorSpec{
		{Name: "unknown", Version: 1},
		{Name: "even", Version: 2},
		{Name: "even", Version: 1, Settings: []Setting{{Key: "extra", Value: "1"}}},
		{Name: "multiply", Version: 1},
		{Name: "multiply", Version: 1, Settings: []Setting{{Key: "factor", Value: "nope"}}},
	}
	for _, operator := range invalid {
		if _, err := ExecuteOperator(operator, input); err == nil {
			t.Fatalf("invalid operator accepted: %#v", operator)
		}
	}
	bad := Tuple{Fields: []Field{{Name: "other", Value: Value{Type: ValueInt64, Int64: 1}}}}
	if _, err := ExecuteOperator(OperatorSpec{Name: "even", Version: 1}, bad); err == nil {
		t.Fatal("wrong field schema accepted")
	}
}
