package bolt

import (
	"encoding/binary"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/boreq/errors"
	"go.etcd.io/bbolt"
)

var errEmptyTopic = errors.New("topic is an empty string")

// TxPublisher uses the provided transaction to publish messages. It can only
// be used during the lifetime of that transaction. If you don't have a running
// transaction but want to publish messages use Publisher. TxPublisher has to
// be initialized by using NewTxPublisher.
type TxPublisher struct {
	tx     *bbolt.Tx
	config PublisherConfig
}

// NewTxPublisher returns an initialized publisher.
func NewTxPublisher(tx *bbolt.Tx, config PublisherConfig) (TxPublisher, error) {
	if tx == nil {
		return TxPublisher{}, errors.New("tx is nil")
	}

	config.setDefaults()

	if err := config.valid(); err != nil {
		return TxPublisher{}, errors.Wrap(err, "invalid config")
	}

	return TxPublisher{
		tx:     tx,
		config: config,
	}, nil
}

// Publish publishes a message. Calling this function with no messages or a
// zero value of topic returns an error.
func (p TxPublisher) Publish(topic string, messages ...*message.Message) error {
	if len(messages) == 0 {
		return errors.New("tried publishing zero messages")
	}

	if topic == "" {
		return errEmptyTopic
	}

	bucket := p.getSubscriptionsBucket(topic)
	if bucket == nil {
		return nil
	}

	marshalledMessages, err := p.marshalMessages(messages)
	if err != nil {
		return errors.Wrap(err, "could not marshal messages")
	}

	if err := bucket.ForEach(func(key, value []byte) error {
		if value != nil {
			return errors.New("encountered a value in the subscriptions bucket")
		}

		subscriptionBucket := bucket.Bucket(key)
		if subscriptionBucket == nil {
			return errors.Wrap(err, "could not get the subscription bucket")
		}

		if err := p.putMessages(subscriptionBucket, marshalledMessages); err != nil {
			return errors.Wrap(err, "could not put messages in the bucket")
		}

		return nil
	}); err != nil {
		return errors.Wrap(err, "failed to publish to all subscriptions")
	}

	return nil
}

// Close does not have to be called and is here just to satisfy the publisher
// interface.
func (p TxPublisher) Close() error {
	return nil
}

func (p TxPublisher) marshalMessages(messages []*message.Message) ([][]byte, error) {
	var result [][]byte

	for _, msg := range messages {
		persistedMessage := PersistedMessage{
			UUID:     msg.UUID,
			Metadata: msg.Metadata,
			Payload:  msg.Payload,
			Created:  time.Now(),
		}

		b, err := p.config.Common.Marshaler.Marshal(persistedMessage)
		if err != nil {
			return nil, errors.Wrap(err, "marshaling failed")
		}

		result = append(result, b)
	}

	return result, nil
}

func (p TxPublisher) putMessages(bucket *bbolt.Bucket, marshaledMessages [][]byte) error {
	for _, marshaledMessage := range marshaledMessages {
		key, err := p.nextMessageKey(bucket)
		if err != nil {
			return errors.Wrap(err, "could not get the next message key")
		}

		if err := bucket.Put(key, marshaledMessage); err != nil {
			return errors.Wrap(err, "could not put the message in the bucket")
		}
	}

	return nil
}

func (p TxPublisher) nextMessageKey(bucket *bbolt.Bucket) ([]byte, error) {
	nextSequence, err := bucket.NextSequence()
	if err != nil {
		return nil, errors.Wrap(err, "could not get the next sequence")
	}

	return itob(nextSequence), nil
}

// getSubscriptionsBucket may return nil, if that happens then there are no
// subscriptions for this topic.
func (p TxPublisher) getSubscriptionsBucket(topic string) *bbolt.Bucket {
	bucketNames := subscriptionsBucketTree(p.config.Common.Bucket, topic)

	bucket := p.tx.Bucket(bucketNames[0])
	if bucket == nil {
		return nil
	}

	for i := 1; i < len(bucketNames); i++ {
		bucketName := bucketNames[i]
		bucket = bucket.Bucket(bucketName)
		if bucket == nil {
			return nil
		}
	}

	return bucket
}

func itob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}
