package model

import (
	"errors"
	"sort"
	"strconv"
	"strings"
	"unicode"
)

// maxWordsPerLine bounds the words one "lines" source tuple carries so that
// split_words never exceeds MaxOperatorOutputs.
const maxWordsPerLine = 16

// builtinCorpora are the deterministic public-domain texts the "lines"
// source can emit; embedding them keeps source replay byte-identical on
// every node without any file dependency.
var builtinCorpora = map[string]string{
	"gettysburg": "Four score and seven years ago our fathers brought forth on this continent, a new nation, conceived in Liberty, and dedicated to the proposition that all men are created equal. " +
		"Now we are engaged in a great civil war, testing whether that nation, or any nation so conceived and so dedicated, can long endure. We are met on a great battle-field of that war. " +
		"We have come to dedicate a portion of that field, as a final resting place for those who here gave their lives that that nation might live. It is altogether fitting and proper that we should do this. " +
		"But, in a larger sense, we can not dedicate, we can not consecrate, we can not hallow this ground. The brave men, living and dead, who struggled here, have consecrated it, far above our poor power to add or detract. " +
		"The world will little note, nor long remember what we say here, but it can never forget what they did here. It is for us the living, rather, to be dedicated here to the unfinished work which they who fought here have thus far so nobly advanced. " +
		"It is rather for us to be here dedicated to the great task remaining before us, that from these honored dead we take increased devotion to that cause for which they gave the last full measure of devotion, " +
		"that we here highly resolve that these dead shall not have died in vain, that this nation, under God, shall have a new birth of freedom, and that government of the people, by the people, for the people, shall not perish from the earth.",
	"crane": "Crane moves every tuple exactly once from a source through transforms to a collect sink. Workers keep durable custody of each delivery in a checksummed write ahead log and answer duplicates from that log. " +
		"A leader elected through Raft fences workers by epoch, reconciles assignments, and repairs result replicas after failures. Two current copies of every result must agree before a job seals. " +
		"Membership comes from SWIM with incarnations and tombstones, and never by itself reassigns work. Bounded frames, bounded jobs, and bounded stores turn every overload into a typed retryable error.",
}

// CorpusNames lists the built-in corpora accepted by the "lines" source.
func CorpusNames() []string {
	names := make([]string, 0, len(builtinCorpora))
	for name := range builtinCorpora {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// corpusChunks splits one corpus into deterministic lines of at most
// maxWordsPerLine whitespace-separated words.
func corpusChunks(text string) []string {
	words := strings.Fields(text)
	chunks := make([]string, 0, len(words)/maxWordsPerLine+1)
	for start := 0; start < len(words); start += maxWordsPerLine {
		end := start + maxWordsPerLine
		if end > len(words) {
			end = len(words)
		}
		chunks = append(chunks, strings.Join(words[start:end], " "))
	}
	return chunks
}

// linesSettings validates a "lines" source and returns its corpus chunks.
func linesSettings(operator OperatorSpec) ([]string, error) {
	descriptor, err := validateOperatorSpec(operator)
	if err != nil || descriptor.Role != OperatorRoleSource || operator.Name != "lines" {
		return nil, errors.New("invalid lines operator")
	}
	text, ok := builtinCorpora[operator.Settings[0].Value]
	if !ok {
		return nil, errors.New("unknown corpus " + strconv.Quote(operator.Settings[0].Value) + "; known: " + strings.Join(CorpusNames(), ", "))
	}
	return corpusChunks(text), nil
}

// linesTuple emits the sequence-th line owned by one source partition; lines
// are dealt round-robin across partitions exactly like range ordinals.
func linesTuple(operator OperatorSpec, parallelism, partition uint16, sequence uint64) (Tuple, bool, error) {
	chunks, err := linesSettings(operator)
	if err != nil {
		return Tuple{}, false, err
	}
	ordinal := (sequence-1)*uint64(parallelism) + uint64(partition)
	if ordinal >= uint64(len(chunks)) {
		return Tuple{}, false, nil
	}
	return Tuple{Fields: []Field{{Name: "line", Value: Value{Type: ValueString, String: chunks[ordinal]}}}}, true, nil
}

// splitWords emits one lowercase word tuple per alphabetic word of a line
// tuple, dropping punctuation and digits; a line carries at most
// maxWordsPerLine words so the output stays within MaxOperatorOutputs.
func splitWords(input Tuple) ([]Tuple, error) {
	if len(input.Fields) != 1 || input.Fields[0].Name != "line" || input.Fields[0].Value.Type != ValueString {
		return nil, errors.New("split_words requires exactly one string line field")
	}
	outputs := make([]Tuple, 0, maxWordsPerLine)
	for _, raw := range strings.Fields(input.Fields[0].Value.String) {
		word := strings.Map(func(r rune) rune {
			if unicode.IsLetter(r) {
				return unicode.ToLower(r)
			}
			return -1
		}, raw)
		if word == "" {
			continue
		}
		if uint64(len(outputs)) == LimitsV1().MaxOperatorOutputs {
			return nil, errors.New("split_words line exceeds the operator output bound")
		}
		outputs = append(outputs, Tuple{Fields: []Field{{Name: "word", Value: Value{Type: ValueString, String: word}}}})
	}
	return outputs, nil
}

// minLength passes word tuples whose word has at least the configured
// number of characters and drops the rest.
func minLength(operator OperatorSpec, input Tuple) ([]Tuple, error) {
	if len(input.Fields) != 1 || input.Fields[0].Name != "word" || input.Fields[0].Value.Type != ValueString {
		return nil, errors.New("min_length requires exactly one string word field")
	}
	length, err := strconv.ParseInt(operator.Settings[0].Value, 10, 64)
	if err != nil {
		return nil, err
	}
	if int64(len([]rune(input.Fields[0].Value.String))) < length {
		return []Tuple{}, nil
	}
	return []Tuple{cloneTuple(input)}, nil
}
