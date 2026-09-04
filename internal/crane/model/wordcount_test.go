package model

import (
	"strings"
	"testing"
)

func linesOperator(corpus string) OperatorSpec {
	return OperatorSpec{Name: "lines", Version: 1, Settings: []Setting{{Key: "corpus", Value: corpus}}}
}

func TestLinesSourceDealsCorpusChunksAcrossPartitionsDeterministically(t *testing.T) {
	chunks, err := linesSettings(linesOperator("gettysburg"))
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) == 0 {
		t.Fatal("corpus produced no lines")
	}
	for _, chunk := range chunks {
		if words := len(strings.Fields(chunk)); words == 0 || words > maxWordsPerLine {
			t.Fatalf("chunk %q has %d words, want 1..%d", chunk, words, maxWordsPerLine)
		}
	}
	eofs, err := rangeEOFs(linesOperator("gettysburg"), 3)
	if err != nil {
		t.Fatal(err)
	}
	total := uint64(0)
	for partition, eof := range eofs {
		total += eof
		for sequence := uint64(1); sequence <= eof; sequence++ {
			tuple, ok, err := linesTuple(linesOperator("gettysburg"), 3, uint16(partition), sequence)
			if err != nil || !ok {
				t.Fatalf("partition %d sequence %d: ok=%v err=%v", partition, sequence, ok, err)
			}
			if want := chunks[(sequence-1)*3+uint64(partition)]; tuple.Fields[0].Value.String != want {
				t.Fatalf("partition %d sequence %d = %q, want %q", partition, sequence, tuple.Fields[0].Value.String, want)
			}
		}
		if _, ok, _ := linesTuple(linesOperator("gettysburg"), 3, uint16(partition), eof+1); ok {
			t.Fatalf("partition %d emitted past its EOF", partition)
		}
	}
	if total != uint64(len(chunks)) {
		t.Fatalf("partition EOFs sum to %d, want %d chunks", total, len(chunks))
	}
	if _, err := linesSettings(linesOperator("no-such-corpus")); err == nil {
		t.Fatal("unknown corpus accepted")
	}
}

func TestSplitWordsNormalizesAndBoundsOutputs(t *testing.T) {
	line := Tuple{Fields: []Field{{Name: "line", Value: Value{Type: ValueString, String: "Four score, and SEVEN years-ago; 1863!"}}}}
	outputs, err := ExecuteOperator(OperatorSpec{Name: "split_words", Version: 1}, line)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]string, 0, len(outputs))
	for _, output := range outputs {
		got = append(got, output.Fields[0].Value.String)
	}
	if strings.Join(got, " ") != "four score and seven yearsago" {
		t.Fatalf("split words = %v", got)
	}
	long := Tuple{Fields: []Field{{Name: "line", Value: Value{Type: ValueString, String: strings.Repeat("word ", int(LimitsV1().MaxOperatorOutputs)+1)}}}}
	if _, err := ExecuteOperator(OperatorSpec{Name: "split_words", Version: 1}, long); err == nil {
		t.Fatal("line beyond the output bound was accepted")
	}
	if _, err := ExecuteOperator(OperatorSpec{Name: "split_words", Version: 1}, intTupleValue(1)); err == nil {
		t.Fatal("integer tuple accepted by split_words")
	}
}

func TestMinLengthDropsShortWords(t *testing.T) {
	operator := OperatorSpec{Name: "min_length", Version: 1, Settings: []Setting{{Key: "length", Value: "4"}}}
	for word, keep := range map[string]bool{"the": false, "four": true, "nation": true} {
		outputs, err := ExecuteOperator(operator, Tuple{Fields: []Field{{Name: "word", Value: Value{Type: ValueString, String: word}}}})
		if err != nil {
			t.Fatal(err)
		}
		if (len(outputs) == 1) != keep {
			t.Fatalf("word %q kept=%v, want %v", word, len(outputs) == 1, keep)
		}
	}
}

func TestWordCountTopologyValidates(t *testing.T) {
	spec := TopologySpec{SchemaVersion: 1, Name: "word-count", RegistryFingerprint: RegistryFingerprint(), Stages: []StageSpec{
		{StageID: 1, Name: "lines", Role: StageSource, Parallelism: 2, Operator: linesOperator("gettysburg")},
		{StageID: 2, Name: "words", Role: StageTransform, Parallelism: 2, Operator: OperatorSpec{Name: "split_words", Version: 1}},
		{StageID: 3, Name: "long", Role: StageTransform, Parallelism: 2, Operator: OperatorSpec{Name: "min_length", Version: 1, Settings: []Setting{{Key: "length", Value: "4"}}}},
		{StageID: 4, Name: "counted", Role: StageSink, Parallelism: 1, Operator: OperatorSpec{Name: "collect", Version: 1}},
	}, Edges: []EdgeSpec{
		{EdgeID: 1, SourceStageID: 1, DestinationStageID: 2, Routing: RoutingShuffle},
		{EdgeID: 2, SourceStageID: 2, DestinationStageID: 3, Routing: RoutingShuffle},
		{EdgeID: 3, SourceStageID: 3, DestinationStageID: 4, Routing: RoutingShuffle},
	}}
	if _, err := ValidateTopology(spec); err != nil {
		t.Fatalf("word-count topology rejected: %v", err)
	}
}
