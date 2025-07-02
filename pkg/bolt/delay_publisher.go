package bolt

import (
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/components/delay"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/boreq/errors"
	"go.etcd.io/bbolt"
)

// DelayedBoltPublisher publishes messages with delay functionality. Messages
// with delay metadata are stored with their scheduled time as the bucket key,
// allowing the DelayedBoltSubscriber to retrieve only messages whose time
// has arrived.
type DelayedBoltPublisher struct {
	db     *bbolt.DB
	config PublisherConfig
}

// NewDelayedPublisher creates a new DelayedBoltPublisher that wraps the provided publisher.
func NewDelayedPublisher(db *bbolt.DB, config PublisherConfig) (*DelayedBoltPublisher, error) {
	if db == nil {
		return nil, errors.New("db is nil")
	}

	config.setDefaults()

	if err := config.valid(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	return &DelayedBoltPublisher{
		db:     db,
		config: config,
	}, nil
}

// Publish publishes messages, handling delayed messages specially by storing them
// in separate buckets with time-based keys for efficient retrieval.
func (p *DelayedBoltPublisher) Publish(topic string, messages ...*message.Message) error {
	if len(messages) == 0 {
		return errors.New("tried publishing zero messages")
	}

	if topic == "" {
		return errEmptyTopic
	}

	return p.db.Batch(func(tx *bbolt.Tx) error {
		if err := p.storeDelayedMessages(tx, topic, messages); err != nil {
			return errors.Wrap(err, "failed to store delayed messages")
		}

		return nil
	})
}

// storeDelayedMessages stores delayed messages in all subscription buckets
func (p *DelayedBoltPublisher) storeDelayedMessages(tx *bbolt.Tx, topic string, messages []*message.Message) error {
	subscriptionsBucket := p.getSubscriptionsBucket(tx, topic)
	if subscriptionsBucket == nil {
		return nil
	}

	marshalledMessages, err := p.marshalMessages(messages)
	if err != nil {
		return errors.Wrap(err, "could not marshal messages")
	}

	if err := subscriptionsBucket.ForEach(func(key, value []byte) error {
		if value != nil {
			return errors.New("encountered a value in the subscriptions bucket")
		}

		subscriptionBucket := subscriptionsBucket.Bucket(key)
		if subscriptionBucket == nil {
			return errors.New("could not get the subscription bucket")
		}

		if err := p.putDelayedMessages(subscriptionBucket, marshalledMessages); err != nil {
			return errors.Wrap(err, "could not put messages in the bucket")
		}

		return nil
	}); err != nil {
		return errors.Wrap(err, "failed to publish to all subscriptions")
	}

	return nil
}

func (p *DelayedBoltPublisher) marshalMessages(messages []*message.Message) ([]rawMessage, error) {
	var result []rawMessage

	for _, msg := range messages {
		until := msg.Metadata.Get(delay.DelayedUntilKey)
		if until == "" {
			p.config.Common.Logger.Info("WARN: delayed until key is empty", watermill.LogFields{
				"uuid": msg.UUID,
			})
			until = time.Now().UTC().Format(time.RFC3339)
		}

		pmsg := PersistedMessage{
			UUID:     msg.UUID,
			Metadata: msg.Metadata,
			Payload:  msg.Payload,
			Created:  time.Now(),
		}

		messageBytes, err := p.config.Common.Marshaler.Marshal(pmsg)
		if err != nil {
			return nil, errors.Wrap(err, "failed to marshal delayed message")
		}

		result = append(result, rawMessage{
			Key:   []byte(until),
			Value: messageBytes,
		})
	}

	return result, nil
}

func (p *DelayedBoltPublisher) putDelayedMessages(bucket *bbolt.Bucket, marshalledMessages []rawMessage) error {
	for _, msg := range marshalledMessages {
		if err := bucket.Put(msg.Key, msg.Value); err != nil {
			return errors.Wrap(err, "failed to store delayed message")
		}
	}

	return nil
}

// getSubscriptionsBucket gets the subscriptions bucket for a topic
func (p *DelayedBoltPublisher) getSubscriptionsBucket(tx *bbolt.Tx, topic string) *bbolt.Bucket {
	bucketNames := subscriptionsBucketTree(p.config.Common.Bucket, topic)

	bucket := tx.Bucket(bucketNames[0])
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

// Close does not have to be called and is here just to satisfy the publisher
// interface.
func (p *DelayedBoltPublisher) Close() error {
	return nil
}
