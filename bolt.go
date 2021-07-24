// Package bolt implements a Pub/Sub for the Watermill project which uses the
// Bolt database.
//
// Apart from the subscriber there are two publishers available, one which uses
// a provided transaction and one that creates its own transaction.
//
//
//    import(
//            bolt "github.com/boreq/watermill-bolt"
//            "go.etcd.io/bbolt"
//    )
//
//    commonConfig := bolt.CommonConfig{
//            Bucket: []bolt.BucketName{
//                    bolt.BucketName("watermill"),
//            },
//    }
//
//    publisher, err := bolt.NewPublisher(db, bolt.PublisherConfig{
//            Common: commonConfig,
//    })
//
//    subscriber, err := bolt.NewSubscriber(db, bolt.SubscriberConfig{
//            Common: commonConfig,
//    })
//
package bolt

import (
	"github.com/ThreeDotsLabs/watermill"
	"github.com/boreq/errors"
)

// BucketName must have a positive length.
type BucketName []byte

// CommonConfig defines configuration needed by both the subscriber and the
// publisher.
type CommonConfig struct {
	// Bucket specifies the parent bucket in which topics and subscriptions
	// will be stored. The first element is the name of the parent bucket,
	// second is the name of its first child etc. It has to have at least
	// one element.
	Bucket []BucketName

	// Defaults to JSONMarshaler.
	Marshaler Marshaler

	// Defaults to watermill.NopLogger.
	Logger watermill.LoggerAdapter
}

func (config *CommonConfig) setDefaults() {
	if config.Marshaler == nil {
		config.Marshaler = JSONMarshaler{}
	}

	if config.Logger == nil {
		config.Logger = watermill.NopLogger{}
	}
}

func (config CommonConfig) valid() error {
	if len(config.Bucket) == 0 {
		return errors.New("bucket has to have at least one element")
	}

	for _, bucketName := range config.Bucket {
		if len(bucketName) == 0 {
			return errors.New("all bucket names must have a length of at least 1")
		}
	}

	if config.Marshaler == nil {
		return errors.New("marshaler is nil")
	}

	if config.Logger == nil {
		return errors.New("logger is nil")
	}

	return nil
}

type PublisherConfig struct {
	Common CommonConfig
}

func (config *PublisherConfig) setDefaults() {
	config.Common.setDefaults()
}

func (config PublisherConfig) valid() error {
	if err := config.Common.valid(); err != nil {
		return errors.Wrap(err, "common config is not valid")
	}

	return nil
}

type GenerateSubscriptionNameFn func(topic string) string

func defaultGenerateSubscriptionNameFn(topic string) string {
	return topic + "_sub"
}

type SubscriberConfig struct {
	Common CommonConfig

	// GenerateSubscriptionName is used to create a unique identifier for
	// subscriptions created by the subscriber. The names created by
	// multiple subscribers have to be unique within a single topic. Once
	// you set this function for a particular subscriber you should not
	// change it in the future to avoid accidently abandoning your old
	// subscriptions.
	//
	// If only one subscriber is used to listen to various topics using
	// your database then it is perfectly fine to use the default value or
	// write a function that returns a contant string, for example the name
	// of your application.
	//
	// Defaults to topic + "_sub".
	GenerateSubscriptionName GenerateSubscriptionNameFn
}

func (config *SubscriberConfig) setDefaults() {
	config.Common.setDefaults()

	if config.GenerateSubscriptionName == nil {
		config.GenerateSubscriptionName = defaultGenerateSubscriptionNameFn
	}
}

func (config SubscriberConfig) valid() error {
	if err := config.Common.valid(); err != nil {
		return errors.Wrap(err, "common config is not valid")
	}

	if config.GenerateSubscriptionName == nil {
		return errors.New("function generating subscription names is not set")
	}

	return nil
}

// - buckets...
//   - topics
//     - <topic_name>
//       - subscriptions
//         - <subscription_name>
func subscriptionsBucketTree(buckets []BucketName, topic string) []BucketName {
	return append(
		buckets,
		BucketName("topics"),
		BucketName(topic),
		BucketName("subscriptions"),
	)
}
