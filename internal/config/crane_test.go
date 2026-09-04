package config

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"crane/internal/crane/model"
)

func TestDefaultCraneConfigPinsOperationalDefaultsAndCompiledFingerprint(t *testing.T) {
	want := CraneConfig{
		WorkerSlots:                  4,
		WorkerControlTimeout:         Duration(2 * time.Second),
		TupleRetryInterval:           Duration(200 * time.Millisecond),
		TupleCompletionRetryInterval: Duration(time.Second),
		FailureGracePeriod:           Duration(5 * time.Second),
		MaxWorkerStoreBytes:          1 << 30,
		ConsensusFingerprint:         "2b98e62384afd912506b032e751aba4504e905fe5d6cec53754a6675e9b93435",
	}
	if got := DefaultCraneConfig(); got != want {
		t.Fatalf("DefaultCraneConfig() = %#v, want %#v", got, want)
	}
}

func TestDecodeCraneDefaultsOmittedOperationalFieldsButNeverItsFingerprint(t *testing.T) {
	configuration := validConfig(createSecret(t, 0o600))
	encoded, err := json.Marshal(configuration)
	if err != nil {
		t.Fatal(err)
	}
	defaultCrane, err := json.Marshal(DefaultCraneConfig())
	if err != nil {
		t.Fatal(err)
	}
	partialCrane := `{"consensus_fingerprint":"` + model.ConsensusFingerprintHex() + `"}`
	partial := strings.Replace(string(encoded), string(defaultCrane), partialCrane, 1)
	decoded, err := Decode(strings.NewReader(partial))
	if err != nil {
		t.Fatalf("Decode partial Crane config: %v", err)
	}
	if got, want := decoded.Crane, DefaultCraneConfig(); got != want {
		t.Fatalf("decoded Crane = %#v, want defaults %#v", got, want)
	}

	unknown := strings.Replace(string(encoded), `"crane":{`, `"crane":{"unknown":true,`, 1)
	if _, err := Decode(strings.NewReader(unknown)); err == nil {
		t.Fatal("Decode accepted unknown nested Crane JSON field")
	}
	missingFingerprint := strings.Replace(string(encoded), model.ConsensusFingerprintHex(), "", 1)
	if _, err := Decode(strings.NewReader(missingFingerprint)); err == nil {
		t.Fatal("Decode defaulted an omitted Crane consensus fingerprint")
	}
}

func TestCraneConfigValidateRejectsOutOfContractFields(t *testing.T) {
	valid := DefaultCraneConfig()
	for _, test := range []struct {
		name   string
		mutate func(*CraneConfig)
	}{
		{name: "zero slots", mutate: func(c *CraneConfig) { c.WorkerSlots = 0 }},
		{name: "slots above maximum", mutate: func(c *CraneConfig) { c.WorkerSlots = 257 }},
		{name: "control timeout below minimum", mutate: func(c *CraneConfig) { c.WorkerControlTimeout = Duration(99 * time.Millisecond) }},
		{name: "custody retry above maximum", mutate: func(c *CraneConfig) { c.TupleRetryInterval = Duration(10*time.Second + time.Nanosecond) }},
		{name: "completion retry before custody retry", mutate: func(c *CraneConfig) { c.TupleCompletionRetryInterval = c.TupleRetryInterval - 1 }},
		{name: "failure grace below checked double timeout", mutate: func(c *CraneConfig) { c.FailureGracePeriod = c.WorkerControlTimeout*2 - 1 }},
		{name: "store below minimum", mutate: func(c *CraneConfig) { c.MaxWorkerStoreBytes = (1 << 20) - 1 }},
		{name: "store above maximum", mutate: func(c *CraneConfig) { c.MaxWorkerStoreBytes = (1 << 40) + 1 }},
		{name: "uppercase fingerprint", mutate: func(c *CraneConfig) {
			c.ConsensusFingerprint = "C7E089BCBD46DEF764778DF97F4C4021B386CE7E1EAF8008BDA7E147E28BA7A8"
		}},
		{name: "wrong fingerprint", mutate: func(c *CraneConfig) {
			c.ConsensusFingerprint = "0000000000000000000000000000000000000000000000000000000000000000"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := valid
			test.mutate(&got)
			if err := got.Validate(); err == nil {
				t.Fatal("Validate accepted an invalid Crane configuration")
			}
		})
	}
}

func TestCraneControlEndpointFromRaftUsesCanonicalCheckedOffset(t *testing.T) {
	for _, test := range []struct {
		name string
		raw  string
		want Endpoint
	}{
		{name: "dns", raw: "Node.Example.Test.:8008", want: Endpoint{Host: "node.example.test", Port: 8006}},
		{name: "ipv4", raw: "192.0.2.10:8008", want: Endpoint{Host: "192.0.2.10", Port: 8006}},
		{name: "ipv6", raw: "[2001:0db8::1]:8008", want: Endpoint{Host: "2001:db8::1", Port: 8006}},
	} {
		t.Run(test.name, func(t *testing.T) {
			raftEndpoint, err := ParseRoutableEndpoint(test.raw)
			if err != nil {
				t.Fatalf("ParseRoutableEndpoint(%q): %v", test.raw, err)
			}
			got, err := CraneControlEndpointFromRaft(raftEndpoint)
			if err != nil {
				t.Fatalf("CraneControlEndpointFromRaft(%#v): %v", raftEndpoint, err)
			}
			if got != test.want {
				t.Fatalf("CraneControlEndpointFromRaft(%#v) = %#v, want %#v", raftEndpoint, got, test.want)
			}
		})
	}
	for _, endpoint := range []Endpoint{{Host: "node.example.test", Port: 7}, {Host: "bad host", Port: 8008}} {
		if _, err := CraneControlEndpointFromRaft(endpoint); err == nil {
			t.Fatalf("CraneControlEndpointFromRaft(%#v) succeeded", endpoint)
		}
	}
}

func TestNodeConfigRequiresCanonicalRaftVoterEndpointsAndCraneSnapshotBudget(t *testing.T) {
	for _, endpoint := range []string{"Node.Example.Test.:8008", "[2001:0db8:0:0:0:0:0:1]:8008"} {
		configuration := validConfig(createSecret(t, 0o600))
		configuration.RaftVoters[1].Endpoint = endpoint
		if err := configuration.Validate(); err == nil {
			t.Fatalf("Validate accepted non-canonical voter endpoint %q", endpoint)
		}
	}
	configuration := validConfig(createSecret(t, 0o600))
	configuration.Raft.MaxSnapshotBytes = model.LimitsV1().MaxSnapshotBytes - 1
	if err := configuration.Validate(); err == nil {
		t.Fatal("Validate accepted a voter Raft snapshot budget below the Crane minimum")
	}
	configuration.NodeID = 4
	if err := configuration.Validate(); err != nil {
		t.Fatalf("Validate rejected a nonvoter with a local Raft snapshot budget below Crane's voter minimum: %v", err)
	}
}
