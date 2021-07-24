package bolt

import (
	"encoding/json"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
)

// PersistedMessage is marshalled and unmarshalled for storage in the database.
type PersistedMessage struct {
	UUID     string
	Metadata message.Metadata
	Payload  message.Payload
	Created  time.Time
}

// Marshaler is responsible for marshalling and unmarshaling messages for
// storage. Implementations need to marshal and unmarshal all fields of the
// persisted message.
type Marshaler interface {
	Marshal(msg PersistedMessage) ([]byte, error)
	Unmarshal(b []byte) (PersistedMessage, error)
}

type JSONMarshaler struct {
}

func (m JSONMarshaler) Marshal(msg PersistedMessage) ([]byte, error) {
	v := persistedMessage{
		UUID:     msg.UUID,
		Metadata: msg.Metadata,
		Payload:  msg.Payload,
		Created:  msg.Created,
	}

	return json.Marshal(v)
}

func (m JSONMarshaler) Unmarshal(b []byte) (PersistedMessage, error) {
	var v persistedMessage

	if err := json.Unmarshal(b, &v); err != nil {
		return PersistedMessage{}, err
	}

	msg := message.NewMessage(v.UUID, v.Payload)
	msg.Metadata = v.Metadata

	return PersistedMessage{
		UUID:     v.UUID,
		Metadata: v.Metadata,
		Payload:  v.Payload,
		Created:  v.Created,
	}, nil

}

type persistedMessage struct {
	UUID     string            `json:"uuid,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
	Payload  []byte            `json:"payload,omitempty"`
	Created  time.Time         `json:"created,omitempty"`
}
