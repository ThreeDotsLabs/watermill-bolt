package bolt_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-bolt/pkg/bolt"
	"github.com/ThreeDotsLabs/watermill/components/delay"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

var (
	commonCfg = bolt.CommonConfig{
		Bucket: []bolt.BucketName{[]byte("test")},
		Logger: watermill.NewStdLogger(true, true),
	}
)

func newBoltDB(t *testing.T) *bbolt.DB {
	path := fmt.Sprintf("test_%s.db", t.Name())
	db, err := bbolt.Open(path, 0600, nil)
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, db.Close())
		assert.NoError(t, os.Remove(path))
	})

	return db
}

func TestDelayedBoltPublisherNoDelay(t *testing.T) {
	db := newBoltDB(t)
	pub, err := bolt.NewDelayedPublisher(db, bolt.PublisherConfig{Common: commonCfg})
	require.NoError(t, err)

	sub, err := bolt.NewDelayedSubscriber(db, bolt.SubscriberConfig{Common: commonCfg})
	require.NoError(t, err)

	topic := watermill.NewUUID()

	messages, err := sub.Subscribe(context.Background(), topic)
	require.NoError(t, err)

	msg := message.NewMessage(watermill.NewUUID(), []byte("{}"))

	err = pub.Publish(topic, msg)
	require.NoError(t, err)

	assert.EventuallyWithT(t, func(t *assert.CollectT) {
		select {
		case received := <-messages:
			assert.Equal(t, msg.UUID, received.UUID)
			received.Ack()
		default:
			t.Errorf("message should be received")
		}
	}, time.Second, time.Millisecond*10)
}

func TestDelayedBoltPublisherWithDelay(t *testing.T) {
	db := newBoltDB(t)
	pub, err := bolt.NewDelayedPublisher(db, bolt.PublisherConfig{Common: commonCfg})
	require.NoError(t, err)

	sub, err := bolt.NewDelayedSubscriber(db, bolt.SubscriberConfig{Common: commonCfg})
	require.NoError(t, err)

	topic := watermill.NewUUID()

	messages, err := sub.Subscribe(context.Background(), topic)
	require.NoError(t, err)

	msg := message.NewMessage(watermill.NewUUID(), []byte("{}"))
	delay.Message(msg, delay.For(time.Second*5))
	unwantedMsg := message.NewMessage(watermill.NewUUID(), []byte("{}"))
	delay.Message(unwantedMsg, delay.For(time.Minute))

	err = pub.Publish(topic, unwantedMsg, msg)
	require.NoError(t, err)

	select {
	case <-messages:
		t.Errorf("message should not be received")
	case <-time.After(time.Second * 4):
	}

	assert.EventuallyWithT(t, func(t *assert.CollectT) {
		select {
		case received := <-messages:
			assert.Equal(t, msg.UUID, received.UUID)
			received.Ack()
		default:
			t.Errorf("message should be received")
		}
	}, time.Second, time.Millisecond*10)

	select {
	case <-messages:
		t.Errorf("unwanted message should not be received")
	case <-time.After(time.Second):
	}
}
