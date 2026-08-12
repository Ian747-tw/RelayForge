package domain

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

type fingerprintInput struct {
	EventType   string          `json:"event_type"`
	Payload     json.RawMessage `json:"payload"`
	EndpointIDs []int64         `json:"endpoint_ids"`
}

func RequestHash(params CreateEventParams) ([]byte, error) {
	var payloadValue any

	decoder := json.NewDecoder(bytes.NewReader(params.Payload))
	decoder.UseNumber()

	if err := decoder.Decode(&payloadValue); err != nil {
		return nil, fmt.Errorf("decode payload for fingerprint: %w", err)
	}

	canonicalPayload, err := json.Marshal(payloadValue)
	if err != nil {
		return nil, fmt.Errorf("encode canonical payload: %w", err)
	}

	endpointIDs := append([]int64(nil), params.EndpointIDs...)
	sort.Slice(
		endpointIDs,
		func(i, j int) bool {
			return endpointIDs[i] < endpointIDs[j]
		},
	)

	input := fingerprintInput{
		EventType:   strings.TrimSpace(params.EventType),
		Payload:     canonicalPayload,
		EndpointIDs: endpointIDs,
	}

	encoded, err := json.Marshal(input)
	if err != nil {
		return nil, fmt.Errorf("encoded fingerprint input: %w", err)
	}

	sum := sha256.Sum256(encoded)

	hash := make([]byte, len(sum))
	copy(hash, sum[:])

	return hash, nil
}
