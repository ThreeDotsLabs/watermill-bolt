package bolt

import (
	"bytes"
	"context"
	"time"

	"github.com/boreq/errors"
	"go.etcd.io/bbolt"
)

// DelayedBoltSubscriber extends the regular Subscriber to handle delayed
// messages. It only retrieves messages whose scheduled time has arrived,
// based on the message keys stored as RFC3339 timestamps.
type DelayedBoltSubscriber struct {
	*Subscriber
}

// NewDelayedBoltSubscriber creates an initialized delayed subscriber that only
// delivers messages when their scheduled time has arrived.
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
