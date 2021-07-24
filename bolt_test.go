package bolt_test

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
	bolt "github.com/boreq/watermill-bolt"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func TestBolt(t *testing.T) {
	features := tests.Features{
		ConsumerGroups:                      false,
		ExactlyOnceDelivery:                 false,
		GuaranteedOrder:                     false,
		GuaranteedOrderWithSingleSubscriber: false,
		Persistent:                          true,
		RestartServiceCommand:               nil,
		RequireSingleInstance:               false,
		NewSubscriberReceivesOldMessages:    false,
	}

	db, cleanup := boltFixture(t)
	t.Cleanup(cleanup)

	constructor := func(t *testing.T) (message.Publisher, message.Subscriber) {
		commonConfig := bolt.CommonConfig{
			Bucket: []bolt.BucketName{
				bolt.BucketName("watermill"),
			},
			Logger: watermill.NewStdLogger(true, true),
		}

		publisher, err := bolt.NewPublisher(db, bolt.PublisherConfig{
			Common: commonConfig,
		})
		require.NoError(t, err)

		subscriber, err := bolt.NewSubscriber(db, bolt.SubscriberConfig{
			Common: commonConfig,
		})
		require.NoError(t, err)

		return publisher, subscriber
	}

	tests.TestPubSub(
		t,
		features,
		constructor,
		nil,
	)
}

type cleanupFunc func()

func fileFixture(t *testing.T) (string, cleanupFunc) {
	file, err := ioutil.TempFile("", "eggplant_test")
	if err != nil {
		t.Fatal(err)
	}

	cleanup := func() {
		err := os.Remove(file.Name())
		if err != nil {
			t.Fatal(err)
		}
	}

	return file.Name(), cleanup
}

func boltFixture(t *testing.T) (*bbolt.DB, cleanupFunc) {
	file, fileCleanup := fileFixture(t)

	db, err := bbolt.Open(file, 0600, &bbolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}

	cleanup := func() {
		defer fileCleanup()

		err := db.Close()
		if err != nil {
			t.Fatal(err)
		}
	}

	return db, cleanup
}
