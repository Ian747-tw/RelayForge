package domain

import (
	"bytes"
	"encoding/json"
	"testing"
)

func mustRequestHash(t *testing.T, params CreateEventParams) []byte {
	t.Helper()

	hash, err := RequestHash(params)
	if err != nil {
		t.Fatalf("RequestHash() error: %v", err)
	}

	return hash
}

func TestRequestHash_EquivalentRequestsMatch(t *testing.T) {
	base := CreateEventParams{
		EventType:      "user.created",
		Payload:        json.RawMessage(`{"name":"Ian","age":20}`),
		IdempotencyKey: "key-1",
		EndpointIDs:    []int64{1, 2, 3},
	}

	want := mustRequestHash(t, base)

	tests := []struct {
		name   string
		params CreateEventParams
	}{
		{
			name: "different JSON whitespace",
			params: CreateEventParams{
				EventType:      "user.created",
				Payload:        json.RawMessage(`{ "name": "Ian", "age": 20 }`),
				IdempotencyKey: "key-1",
				EndpointIDs:    []int64{1, 2, 3},
			},
		},
		{
			name: "different JSON key order",
			params: CreateEventParams{
				EventType:      "user.created",
				Payload:        json.RawMessage(`{"age":20,"name":"Ian"}`),
				IdempotencyKey: "key-1",
				EndpointIDs:    []int64{1, 2, 3},
			},
		},
		{
			name: "different endpoint order",
			params: CreateEventParams{
				EventType:      "user.created",
				Payload:        json.RawMessage(`{"name":"Ian","age":20}`),
				IdempotencyKey: "key-1",
				EndpointIDs:    []int64{3, 1, 2},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mustRequestHash(t, tt.params)

			if !bytes.Equal(got, want) {
				t.Errorf(
					"equivalent requests produced different hashes\nwant: %x\ngot:  %x",
					want,
					got,
				)
			}
		})
	}
}
