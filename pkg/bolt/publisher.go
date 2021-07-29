package bolt

import (
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/boreq/errors"
	"go.etcd.io/bbolt"
)

// Publisher publishes messages creating a new transaction every time messages
// are being published. If you already have a running transaction use
// TxPublisher. Publisher has to be initialized by using NewPublisher.
type Publisher struct {
	db     *bbolt.DB
	config PublisherConfig
}

// NewPublisher creates an initialized publisher.
func NewPublisher(db *bbolt.DB, config PublisherConfig) (Publisher, error) {
	if db == nil {
		return Publisher{}, errors.New("db is nil")
	}

	config.setDefaults()

	if err := config.valid(); err != nil {
		return Publisher{}, errors.Wrap(err, "invalid config")
	}

	return Publisher{
		db:     db,
		config: config,
	}, nil
}

// Publish publishes a message. Calling this function with no messages or a
// zero value of topic returns an error.
func (p Publisher) Publish(topic string, messages ...*message.Message) error {
	if err := p.db.Batch(func(tx *bbolt.Tx) error {
		txPublisher, err := NewTxPublisher(tx, p.config)
		if err != nil {
			return errors.Wrap(err, "tx publisher could not be created")
		}
		return txPublisher.Publish(topic, messages...)
	}); err != nil {
		return errors.Wrap(err, "transaction failed")
	}

	return nil
}

// Close does not have to be called and is here just to satisfy the publisher
// interface.
func (p Publisher) Close() error {
	return nil
}
