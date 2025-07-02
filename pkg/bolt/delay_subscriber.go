package bolt

import (
	"bytes"
	"context"
	"time"

	"github.com/boreq/errors"
	"go.etcd.io/bbolt"
)

type DelayedBoltSubscriber struct {
	*Subscriber
}

func NewDelayedBoltSubscriber(db *bbolt.DB, config SubscriberConfig) (*DelayedBoltSubscriber, error) {
	subscriber, err := NewSubscriber(db, config)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create underlying subscriber")
	}

	subscriber.loadMsgsFn = getMessagesWithDelay

	return &DelayedBoltSubscriber{
		subscriber,
	}, nil
}

func getMessagesWithDelay(_ context.Context, s *Subscriber, topic string) ([]rawMessage, error) {
	var messages []rawMessage

	if err := s.db.View(func(tx *bbolt.Tx) error {
		bucket := s.getSubscriptionBucket(tx, topic)
		if bucket == nil {
			return nil
		}

		c := bucket.Cursor()
		now := []byte(time.Now().UTC().Format(time.RFC3339))

		for k, v := c.First(); k != nil && bytes.Compare(k, now) <= 0; k, v = c.Next() {
			messages = append(messages, newRawMessage(k, v))
		}

		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "transaction failed")
	}

	return messages, nil
}
