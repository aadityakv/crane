package protocol

import (
	"reflect"
	"testing"

	"github.com/aaditya/cs425mp3/internal/crane/model"
)

func TestModelPublicControlContractMechanicallyMatchesActualEncoderOrdering(t *testing.T) {
	contract := model.PublicControlContractV1()
	if ControlSchemaVersion != contract.SchemaVersion ||
		MaxControlPayloadBytes+int(model.LimitsV1().AuthenticatedFrameBytes) != int(contract.MaxControlFrameBytes) ||
		MaxResultPageBytes != int(contract.MaxResultPageBytes) ||
		MaxResultPageRecords != int(contract.MaxResultPageRecords) ||
		int(MinEncodedResultRecordBytes) != int(contract.MinEncodedResultRecordBytes) ||
		MaxEncodedResultRecordBytes != int(contract.MaxEncodedResultRecordBytes) ||
		MaxLeaderRedirectEndpoints != int(contract.MaxRedirectEndpoints) ||
		MaxControlEndpointBytes != int(contract.MaxEndpointBytes) ||
		MaxControlErrorDetailBytes != int(contract.MaxErrorDetailBytes) {
		t.Fatalf("protocol constants drifted from public-control contract: %#v", contract)
	}

	messages := controlFixture(t).messages()
	if len(messages) != len(contract.Messages) {
		t.Fatalf("fixture count = %d, contract messages = %d", len(messages), len(contract.Messages))
	}
	for index, message := range messages {
		descriptor := contract.Messages[index]
		if uint16(message.MessageType()) != descriptor.MessageType {
			t.Fatalf("fixture %d type = %d, contract = %d", index, message.MessageType(), descriptor.MessageType)
		}
		layout, err := ControlEncodingLayout(message)
		if err != nil {
			t.Fatalf("ControlEncodingLayout(%d): %v", message.MessageType(), err)
		}
		if !reflect.DeepEqual(layout, descriptor.Fields) {
			t.Fatalf("actual encoder layout for %d\n got: %#v\nwant: %#v", message.MessageType(), layout, descriptor.Fields)
		}
	}
}
