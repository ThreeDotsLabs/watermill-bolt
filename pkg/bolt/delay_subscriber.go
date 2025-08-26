package bolt

import (
	"bytes"
	"context"
	"fmt"
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

// NewDelayedSubscriber creates an initialized delayed subscriber that only
// delivers messages when their scheduled time has arrived.
func NewDelayedSubscriber(db *bbolt.DB, config SubscriberConfig) (*DelayedBoltSubscriber, error) {
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
		now := time.Now().UTC()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			keyTime, err := extractTimestamp(k)
			if err != nil {
				continue
			}

			if keyTime.Before(now) || keyTime.Equal(now) {
				messages = append(messages, newRawMessage(k, v))
			}
		}

		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "transaction failed")
	}

	return messages, nil
}

func extractTimestamp(key []byte) (time.Time, error) {
	idx := bytes.IndexByte(key, '_')
	if idx == -1 {
		return time.Time{}, fmt.Errorf("no underscore found in key")
	}

	timestampStr := string(key[:idx])
	return time.Parse(time.RFC3339, timestampStr)
}
