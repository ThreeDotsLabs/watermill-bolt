# Watermill Bolt Pub/Sub [![CI](https://github.com/ThreeDotsLabs/watermill-bolt/workflows/CI/badge.svg)][actions] [![godocs.io](http://godocs.io/github.com/pkg/errors?status.svg)][godoc]
<img align="right" width="200" src="https://threedots.tech/watermill-io/watermill-logo.png">

This is a Pub/Sub for the [Watermill][watermill] project which uses the [Bolt][bolt] database.

All Pub/Sub implementations can be found at [https://watermill.io/pubsubs/](https://watermill.io/pubsubs/).

Watermill is a Go library for working efficiently with message streams. It is intended
for building event driven applications, enabling event sourcing, RPC over messages,
sagas and basically whatever else comes to your mind. You can use conventional pub/sub
implementations like Kafka or RabbitMQ, but also HTTP or MySQL binlog if that fits your use case.

Documentation: https://watermill.io/

Getting started guide: https://watermill.io/docs/getting-started/

Issues: https://github.com/ThreeDotsLabs/watermill/issues

## Contributing

All contributions are very much welcome. If you'd like to help with Watermill development,
please see [open issues](https://github.com/ThreeDotsLabs/watermill/issues?utf8=%E2%9C%93&q=is%3Aissue+is%3Aopen+)
and submit your pull request via GitHub.

### Testing Locally

Since no external dependencies are needed to run the tests it should be enough
to execute the following command:

```
$ make test
```

## License

[MIT License](./LICENSE)


[watermill]: https://watermill.io/
[bolt]: https://github.com/etcd-io/bbolt
[actions]: https://github.com/ThreeDotsLabs/watermill-bolt/actions
[godoc]: http://godocs.io/github.com/ThreeDotsLabs/watermill-bolt
