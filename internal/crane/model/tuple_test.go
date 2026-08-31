package model

import "testing"

func TestTupleValidateRequiresSortedUniqueBoundedFields(t *testing.T) {
	valid := Tuple{Fields: []Field{
		{Name: "age", Value: Value{Type: ValueInt64, Int64: -2}},
		{Name: "message", Value: Value{Type: ValueString, String: "hé"}},
		{Name: "raw", Value: Value{Type: ValueBytes, Bytes: []byte{0, 0xff}}},
	}}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid tuple rejected: %v", err)
	}

	for name, tuple := range map[string]Tuple{
		"unsorted":             {Fields: []Field{{Name: "z", Value: valid.Fields[0].Value}, {Name: "a", Value: valid.Fields[0].Value}}},
		"duplicate":            {Fields: []Field{{Name: "a", Value: valid.Fields[0].Value}, {Name: "a", Value: valid.Fields[0].Value}}},
		"empty name":           {Fields: []Field{{Name: "", Value: valid.Fields[0].Value}}},
		"invalid field UTF-8":  {Fields: []Field{{Name: string([]byte{0xff}), Value: valid.Fields[0].Value}}},
		"invalid string UTF-8": {Fields: []Field{{Name: "a", Value: Value{Type: ValueString, String: string([]byte{0xff})}}}},
		"unknown tag":          {Fields: []Field{{Name: "a", Value: Value{Type: ValueType(99)}}}},
	} {
		if err := tuple.Validate(); err == nil {
			t.Fatalf("%s tuple accepted", name)
		}
	}
}

func TestTupleValidateRejectsCompletePayloadOverBound(t *testing.T) {
	tooLarge := make([]byte, LimitsV1().MaxTuplePayloadBytes+1)
	tuple := Tuple{Fields: []Field{{Name: "raw", Value: Value{Type: ValueBytes, Bytes: tooLarge}}}}
	if err := tuple.Validate(); err == nil {
		t.Fatal("tuple accepted an oversized bytes payload")
	}
}
