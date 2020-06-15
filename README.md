# node-kafka-client

A wrapper library of [node-rdkafka](https://github.com/Blizzard/node-rdkafka) with extended features

## Install

> Important: We are using `node-rdkafka^2.7.1-2` which is not supporting Node v12 or later. Let's use Node version v8 > v10

`npm install @uiza/node-kafka-client`

## Usage

### Consumer

```js
const { Consumer } = require("@uiza/node-kafka-client");

const consumer = new Consumer({
  // Required options
  name: "test-consumer-name",
  groupId: "test-group-id",
  host: "localhost:9092",
  topic: "test-topic",

  // Optional options
  connectTimeout: 5000,
  mode: "non-flowing",
  intervalFetchMessage: 10, // Only affect for non-flowing mode
  numMsgFetchPerTime: 1, // Only affect for non-flowing mode
  logger: null,
});

consumer.listen(async (data) => {
  console.log("Listen data:", data);
});
```

The consumer inherit all `node-rdkafka` configuration options for you to set or override. Using snippet config below and reference original [node-rdkafka](https://github.com/Blizzard/node-rdkafka) lib for more detail of configuration.

```js
{
  ...
  rdKafkaConfig: {
    debug: "all",
  },
  rdKafkaTopicConfig: {
    "auto.offset.reset": "latest",
  },
}
```

```js
consumer.on("event.log", console.log);
```

### Flowing mode:

- Pros
  - Concurrent handler processing message.
- Cons
  - Might cause OOM (out-of-memory) when start the consumer with the lag offset too much
  - Need to handler duplicated messages.
  - Each consumer will block a thread of libuv.
- Suitable for:
  - Small processing time handler.
  - The result of processing messages is independent.

### Non-flowing:

- Pros:
  - Handle one-by-one messages. The next message just consumed after the previous committed
  - It's not block the thread of libuv.
  - Fully control the interval and number of message to fetch
- Cons:
  - Non-concurrency consume messages, higher latency compare to flowing mode.
- Suitable for:
  - High demand of consistency handling messages or the result of processing a message depends on the previous one
  - Running multiple consumers in one application
  - Long-time-taking handler

### Producer

[will be updated]

## Benchmark

[will be updated]

## Contributing

Feel free to dive in! Open an [issue](https://github.com/uizaio/node-kafka-client/issues) or submit PRs.

## Contributors

- [Alex Co](https://github.com/onimsha)
- [Duy Quach](https://github.com/quachduyy)
- [Phu Nguyen](https://github.com/phunguyen19)
- [Cuong Nguyen](https://github.com/CuongNgMan)

## License

MIT © Uiza
