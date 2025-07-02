package bolt

import (
	"context"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/boreq/errors"
	"go.etcd.io/bbolt"
)

// Subscriber receives messages sent by the publishers.
//
// The current implementation always tries to retrieve all messages at the same
// time which can lead to problems with a very high number of unprocessed
// messages in a single subscription.
//
// Subscriber has to be initialized by using NewSubscriber.
type Subscriber struct {
	db         *bbolt.DB
	config     SubscriberConfig
	closeCh    chan struct{}
	wg         sync.WaitGroup
	loadMsgsFn loadMsgs
}

type loadMsgs func(ctx context.Context, s *Subscriber, topic string) ([]rawMessage, error)

// NewSubscriber creates an initialized subscriber.
func NewSubscriber(db *bbolt.DB, config SubscriberConfig) (*Subscriber, error) {
	if db == nil {
		return nil, errors.New("db is nil")
	}

	config.setDefaults()

	if err := config.valid(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	return &Subscriber{
		db:         db,
		config:     config,
		closeCh:    make(chan struct{}),
		loadMsgsFn: getMessages,
	}, nil
}

// Subscribe returns a channel on which you can receive messages send on the
// specified topic. Calling this function with a zero value of topic returns an
// error. Returned channel is closed after closing the subscriber. Subsequent
// calls to subscribe after the subscriber has been closed should not be
// performed and will return a closed channel but will not return an error.
// Further messages will be available on the channel only after the last
// message retrieved from the channel has been acked or nacked. If the context
// is closed then the channel will be closed.
func (s *Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	if topic == "" {
		return nil, errEmptyTopic
	}

	if err := s.createSubscription(topic); err != nil {
		return nil, errors.Wrap(err, "failed to create the subscription")
	}

	s.wg.Add(1)
	ch := make(chan *message.Message)
	go s.run(ctx, topic, ch)
	return ch, nil
}

// Close closes all channels returned by subscribe and shuts down the
// subscriber. Close blocks until all returned channels are successfully
// closed. Close can be called multiple times but subsequent calls have no
// effect.
func (s *Subscriber) Close() error {
	select {
	case <-s.closeCh:
	default:
		close(s.closeCh)
		s.wg.Wait()
	}
	return nil
}

// SubscribeInitialize satisfies one of Watermill's interfaces. It is not
// necessary to manually call it. The same initialization performed by this
// function is performed by subscribe.
func (s *Subscriber) SubscribeInitialize(topic string) error {
	return s.createSubscription(topic)
}

func (s *Subscriber) run(ctx context.Context, topic string, ch chan<- *message.Message) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	defer s.wg.Done()
	defer close(ch)

	for {
		if err := s.receiveMessages(ctx, topic, ch); err != nil {
			s.config.Common.Logger.Error(
				"failed to receive messages",
				err,
				watermill.LogFields{
					topic: topic,
				},
			)
		}

		select {
		case <-time.After(time.Second):
			continue
		case <-s.closeCh:
			return
		case <-ctx.Done():
			return
		}
	}
}

func (s *Subscriber) receiveMessages(ctx context.Context, topic string, ch chan<- *message.Message) error {
	messages, err := s.loadMsgsFn(ctx, s, topic)
	if err != nil {
		return errors.Wrap(err, "could not get all messages")
	}

	s.config.Common.Logger.Debug(
		"got messages",
		watermill.LogFields{"len": len(messages)},
	)

	for _, rawMessage := range messages {
		if err := s.sendMessage(ctx, topic, ch, rawMessage); err != nil {
			return errors.Wrap(err, "could not send the message")
		}
	}

	return nil
}

func (s *Subscriber) sendMessage(ctx context.Context, topic string, ch chan<- *message.Message, rawMessage rawMessage) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	persistedMsg, err := s.config.Common.Marshaler.Unmarshal(rawMessage.Value)
	if err != nil {
		return errors.Wrap(err, "could not unmarshal the message")
	}

	msg := message.NewMessage(persistedMsg.UUID, persistedMsg.Payload)
	msg.Metadata = persistedMsg.Metadata
	msg.SetContext(ctx)

	select {
	case ch <- msg:
	case <-ctx.Done():
		return ctx.Err()
	case <-s.closeCh:
		return errors.New("subscriber was closed")
	}

	select {
	case <-msg.Acked():
		if err := s.ack(topic, rawMessage.Key); err != nil {
			return errors.Wrap(err, "could not ack")
		}
		return nil
	case <-msg.Nacked():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-s.closeCh:
		return errors.New("subscriber was closed")
	}
}

func (s *Subscriber) ack(topic string, rawMessageKey []byte) error {
	return s.db.Batch(func(tx *bbolt.Tx) error {
		bucket, err := s.getOrCreateSubscriptionBucket(tx, topic)
		if err != nil {
			return errors.Wrap(err, "could not get or create a bucket")
		}

		return bucket.Delete(rawMessageKey)
	})
}

func getMessages(_ context.Context, s *Subscriber, topic string) ([]rawMessage, error) {
	var messages []rawMessage

	if err := s.db.View(func(tx *bbolt.Tx) error {
		bucket := s.getSubscriptionBucket(tx, topic)
		if bucket == nil {
			return nil
		}

		if err := bucket.ForEach(func(key, value []byte) error {
			// key and value have to be copied as they are reused
			messages = append(messages, newRawMessage(key, value))
			return nil
		}); err != nil {
			return errors.Wrap(err, "could not iterate over the bucket")
		}

		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "transaction failed")
	}

	return messages, nil
}

func (s *Subscriber) getOrCreateSubscriptionBucket(tx *bbolt.Tx, topic string) (*bbolt.Bucket, error) {
	bucketNames := s.getSubscriptionBucketTree(topic)

	bucket, err := tx.CreateBucketIfNotExists(bucketNames[0])
	if err != nil {
		return nil, errors.Wrap(err, "could not create the main bucket")
	}

	for i := 1; i < len(bucketNames); i++ {
		bucketName := bucketNames[i]
		bucket, err = bucket.CreateBucketIfNotExists(bucketName)
		if err != nil {
			return nil, errors.Wrap(err, "could not create a child bucket")
		}
	}

	return bucket, nil
}

func (s *Subscriber) getSubscriptionBucket(tx *bbolt.Tx, topic string) *bbolt.Bucket {
	bucketNames := s.getSubscriptionBucketTree(topic)

	var names []string
	for _, name := range bucketNames {
		names = append(names, string(name))
	}

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

func (s *Subscriber) getSubscriptionBucketTree(topic string) []BucketName {
	return append(
		subscriptionsBucketTree(s.config.Common.Bucket, topic),
		BucketName(s.config.GenerateSubscriptionName(topic)),
	)
}

// createSubscription initializes the subscription bucket for the provided
// topic. Only after this is done for the first time will the messages be
// received by this subscriber.
func (s *Subscriber) createSubscription(topic string) error {
	return s.db.Batch(func(tx *bbolt.Tx) error {
		_, err := s.getOrCreateSubscriptionBucket(tx, topic)
		return err
	})
}

type rawMessage struct {
	Key, Value []byte
}

func newRawMessage(key, value []byte) rawMessage {
	msg := rawMessage{
		Key:   make([]byte, len(key)),
		Value: make([]byte, len(value)),
	}

	copy(msg.Key, key)
	copy(msg.Value, value)

	return msg
}
