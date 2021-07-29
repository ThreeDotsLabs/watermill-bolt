package bolt_test

import (
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/boreq/watermill-bolt/pkg/bolt"
	"github.com/stretchr/testify/require"
)

func TestJSONMarshaler(t *testing.T) {
	m := bolt.JSONMarshaler{}
	marshalerTest(t, m)
}

func marshalerTest(t *testing.T, m bolt.Marshaler) {
	msg := bolt.PersistedMessage{
		UUID:    "uuid",
		Payload: []byte("payload"),
		Metadata: message.Metadata{
			"key": "value",
		},
		Created: time.Now(),
	}

	b, err := m.Marshal(msg)
	require.NoError(t, err)

	unmarshaledMessage, err := m.Unmarshal(b)
	require.NoError(t, err)

	require.Equal(t, msg.UUID, unmarshaledMessage.UUID)
	require.Equal(t, msg.Payload, unmarshaledMessage.Payload)
	require.Equal(t, msg.Metadata, unmarshaledMessage.Metadata)
	require.True(t, msg.Created.Equal(unmarshaledMessage.Created))
}
