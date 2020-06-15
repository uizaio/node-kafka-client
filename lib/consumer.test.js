const { EventEmitter } = require("events");

const eventEmitter = new EventEmitter();

const mockConstructor = jest.fn();
let mockRdKafkaConsumer = {};

jest.mock("node-rdkafka", () => ({
  KafkaConsumer: class {
    constructor(...args) {
      mockConstructor(...args);
      return mockRdKafkaConsumer;
    }
  },
}));

const { Consumer } = require("./consumer");

beforeEach(() => {
  mockRdKafkaConsumer = {
    on: eventEmitter.on.bind(eventEmitter),
    once: eventEmitter.once.bind(eventEmitter),
    subscribe: jest.fn(),
    connect: jest.fn(),
    consume: jest.fn((num, callback) => {
      // eslint-disable-next-line no-unused-expressions
      callback && callback(null, []);
    }),
    commitMessageSync: jest.fn(),
  };
  mockRdKafkaConsumer.subscribe.mockClear();
  mockRdKafkaConsumer.connect.mockClear();
  mockRdKafkaConsumer.consume.mockClear();
  mockRdKafkaConsumer.commitMessageSync.mockClear();
});

const sleep = (ms) => new Promise((resolve) => { setTimeout(resolve, ms); });

let consumer;
let actualError;

beforeEach(() => {
  consumer = undefined;
  actualError = undefined;
});

describe("Test validate options", () => {
  test("Option 'name' is required", () => {
    try {
      consumer = new Consumer();
    } catch (error) {
      actualError = error;
    }

    expect(consumer).toBe(undefined);
    expect(actualError.message).toEqual("Error: Option 'name' is required");
  });

  test("Option 'name' is required", () => {
    try {
      consumer = new Consumer({});
    } catch (error) {
      actualError = error;
    }

    expect(consumer).toBe(undefined);
    expect(actualError.message).toEqual("Error: Option 'name' is required");
  });

  test("Option 'host' is required", () => {
    try {
      consumer = new Consumer({ name: "a" });
    } catch (error) {
      actualError = error;
    }

    expect(consumer).toBe(undefined);
    expect(actualError.message).toEqual("Error: Option 'host' is required");
  });

  test("Option 'groupId' is required", () => {
    try {
      consumer = new Consumer({ name: "name", host: "host" });
    } catch (error) {
      actualError = error;
    }

    expect(consumer).toBe(undefined);
    expect(actualError.message).toEqual("Error: Option 'groupId' is required");
  });

  test("Option 'topic' is required", () => {
    try {
      consumer = new Consumer({ name: "name", host: "host", groupId: "groupId" });
    } catch (error) {
      actualError = error;
    }

    expect(consumer).toBe(undefined);
    expect(actualError.message).toEqual("Error: Option 'topic' is required");
  });

  test("Option 'topic' is required", () => {
    try {
      consumer = new Consumer({ name: "name", host: "host", groupId: "groupId" });
    } catch (error) {
      actualError = error;
    }

    expect(consumer).toBe(undefined);
    expect(actualError.message).toEqual("Error: Option 'topic' is required");
  });

  test("Create new consumer success with required options", () => {
    consumer = new Consumer({
      name: "name",
      host: "host",
      groupId: "groupId",
      topic: "topic",
    });

    expect(consumer.options).toEqual({
      name: "name",
      host: "host",
      groupId: "groupId",
      topic: "topic",
      mode: "non-flowing",
      connectTimeout: 5000,
      numMsgFetchPerTime: 1,
      intervalFetchMessage: 10,
      rdKafkaConfig: undefined,
      rdKafkaTopicConfig: undefined,
    });
    expect(mockConstructor).toBeCalledWith(
      {
        "group.id": "groupId",
        "metadata.broker.list": "host",
        "enable.auto.commit": false,
        "socket.keepalive.enable": true,
        event_cb: true,
      },
      {
        "auto.offset.reset": "earliest",
      },
    );
  });

  test("Option 'mode' should be set 'flowing' or 'non-flowing'", () => {
    try {
      consumer = new Consumer({
        name: "name",
        host: "host",
        groupId: "groupId",
        topic: "topic",
        mode: "abc",
      });
    } catch (error) {
      actualError = error;
    }

    expect(consumer).toBe(undefined);
    expect(actualError.message).toEqual("Error: Option 'mode' should be set 'flowing' or 'non-flowing'");
  });

  test("Option 'connectTimeout' should be integer", () => {
    try {
      consumer = new Consumer({
        name: "name",
        host: "host",
        groupId: "groupId",
        topic: "topic",
        connectTimeout: "connectTimeout",
      });
    } catch (error) {
      actualError = error;
    }

    expect(consumer).toBe(undefined);
    expect(actualError.message).toEqual("Error: Option 'connectTimeout' should be integer");
  });

  test("Option 'connectTimeout' should greater than 0", () => {
    try {
      consumer = new Consumer({
        name: "name",
        host: "host",
        groupId: "groupId",
        topic: "topic",
        connectTimeout: -1,
      });
    } catch (error) {
      actualError = error;
    }

    expect(consumer).toBe(undefined);
    expect(actualError.message).toEqual("Error: Option 'connectTimeout' should greater than 0");
  });

  test("Option 'intervalFetchMessage' should be integer", () => {
    try {
      consumer = new Consumer({
        name: "name",
        host: "host",
        groupId: "groupId",
        topic: "topic",
        intervalFetchMessage: "string",
      });
    } catch (error) {
      actualError = error;
    }

    expect(consumer).toBe(undefined);
    expect(actualError.message).toEqual("Error: Option 'intervalFetchMessage' should be integer");
  });

  test("Option 'intervalFetchMessage' should greater than 0", () => {
    try {
      consumer = new Consumer({
        name: "name",
        host: "host",
        groupId: "groupId",
        topic: "topic",
        intervalFetchMessage: -1,
      });
    } catch (error) {
      actualError = error;
    }

    expect(consumer).toBe(undefined);
    expect(actualError.message).toEqual("Error: Option 'intervalFetchMessage' should greater than 0");
  });

  test("Option 'numMsgFetchPerTime' should be integer", () => {
    try {
      consumer = new Consumer({
        name: "name",
        host: "host",
        groupId: "groupId",
        topic: "topic",
        numMsgFetchPerTime: "string",
      });
    } catch (error) {
      actualError = error;
    }

    expect(consumer).toBe(undefined);
    expect(actualError.message).toEqual("Error: Option 'numMsgFetchPerTime' should be integer");
  });

  test("Option 'numMsgFetchPerTime' should greater than 0", () => {
    try {
      consumer = new Consumer({
        name: "name",
        host: "host",
        groupId: "groupId",
        topic: "topic",
        numMsgFetchPerTime: -1,
      });
    } catch (error) {
      actualError = error;
    }

    expect(consumer).toBe(undefined);
    expect(actualError.message).toEqual("Error: Option 'numMsgFetchPerTime' should greater than 0");
  });
});

test("Test support config to node-rdkafka", () => {
  consumer = new Consumer({
    name: "name",
    host: "host",
    groupId: "groupId",
    topic: "topic",
    rdKafkaConfig: {
      "group.id": "1",
      "metadata.broker.list": "2",
      "enable.auto.commit": true,
      "socket.keepalive.enable": false,
      event_cb: false,
      debug: "all",
    },
    rdKafkaTopicConfig: {
      "auto.offset.reset": "latest",
      config: "config",
    },
  });

  expect(consumer).toBeDefined();
  expect(mockConstructor).toBeCalledWith(
    {
      "group.id": "1",
      "metadata.broker.list": "2",
      "enable.auto.commit": true,
      "socket.keepalive.enable": false,
      event_cb: false,
      debug: "all",
    },
    {
      "auto.offset.reset": "latest",
      config: "config",
    },
  );
});

test("Test listen event from node-rdkafka", () => {
  consumer = new Consumer({
    name: "name",
    host: "host",
    groupId: "groupId",
    topic: "topic",
  });

  const mockEventHandler = jest.fn();
  consumer.on("mockEvent", mockEventHandler);
  eventEmitter.emit("mockEvent", "param");
  expect(mockEventHandler).toBeCalledWith("param");
});

test("Test handle and throw error on connection timeout", () => {
  const mockError = new Error("Mock error");
  mockRdKafkaConsumer.connect.mockImplementationOnce((options, callback) => {
    callback(mockError);
  });

  consumer = new Consumer({
    name: "name",
    host: "host",
    groupId: "groupId",
    topic: "topic",
  });

  try {
    consumer.listen(() => {});
  } catch (error) {
    actualError = error;
  }

  expect(actualError.message).toBe(`Cannot connect to Kafka.\n${mockError.stack}`);
});

test("Should throw error if broker transport failure", () => {
  consumer = new Consumer({
    name: "name",
    host: "host",
    groupId: "groupId",
    topic: "topic",
  });

  consumer.listen(() => {});
  try {
    eventEmitter.emit("event.error", new Error("Local: Broker transport failure"));
  } catch (error) {
    actualError = error;
  }

  expect(actualError.message).toBe("Local: Broker transport failure");
});

test("Should log warning if event.error is not broker transport failure", () => {
  const mockOptions = {
    name: "name",
    host: "host",
    groupId: "groupId",
    topic: "topic",
    logger: { debug: jest.fn(), warn: jest.fn() },
  };
  consumer = new Consumer(mockOptions);

  consumer.listen(() => {});
  const mockError = new Error("Local: Other error");
  eventEmitter.emit("event.error", mockError);

  expect(mockOptions.logger.warn).toBeCalledWith(mockError);
});

describe("Test flowing mode", () => {
  test("Test non-flowing mode consume message", async () => {
    const mockMessage = { offset: 1 };
    mockRdKafkaConsumer.consume
      .mockImplementationOnce((num, cb) => {
        cb(null, [mockMessage, mockMessage]);
      })
      .mockImplementationOnce((num, cb) => {
        cb(null, [mockMessage, mockMessage]);
      });

    consumer = new Consumer({
      name: "name",
      host: "host",
      groupId: "groupId",
      topic: "topic",
      intervalFetchMessage: 500,
    });

    const mockTrackHandler = jest.fn();
    consumer.listen(mockTrackHandler);
    eventEmitter.emit("ready");

    await sleep(800);

    expect(mockRdKafkaConsumer.consume).toBeCalledTimes(2);
    expect(mockRdKafkaConsumer.subscribe).toBeCalledWith(["topic"]);
    expect(mockTrackHandler).toBeCalledTimes(4);
    expect(mockRdKafkaConsumer.commitMessageSync).toBeCalledTimes(4);
    expect(mockTrackHandler).toBeCalledWith(mockMessage);
  });
});

describe("Test flowing mode", () => {
  test("Should subscribe on topic and start consume", () => {
    consumer = new Consumer({
      name: "name",
      host: "host",
      groupId: "groupId",
      topic: "topic",
      mode: "flowing",
    });

    consumer.listen(() => {});
    eventEmitter.emit("ready");

    expect(mockRdKafkaConsumer.subscribe).toBeCalledWith(["topic"]);
    expect(mockRdKafkaConsumer.consume).toBeCalledWith();
  });

  test("Test consume message", async () => {
    consumer = new Consumer({
      name: "name",
      host: "host",
      groupId: "groupId",
      topic: "topic",
      mode: "flowing",
    });

    const mockHandler = jest.fn();
    consumer.listen(mockHandler);
    eventEmitter.emit("ready");
    eventEmitter.emit("data", "message");

    expect(mockHandler).toBeCalledWith("message");
  });

  test("Commit message in order", () => {
    consumer = new Consumer({
      name: "name",
      host: "host",
      groupId: "groupId",
      topic: "topic",
      mode: "flowing",
    });

    consumer.flowingCommitQueue = [
      { message: { offset: 1 }, done: false },
      { message: { offset: 2 }, done: false },
      { message: { offset: 3 }, done: false },
      { message: { offset: 4 }, done: false },
    ];

    consumer.flowingCommit({ offset: 2 });
    consumer.flowingCommit({ offset: 3 });

    expect(consumer.flowingCommitQueue.length).toBe(4);
    expect(consumer.flowingCommitQueue[0]).toEqual({
      message: { offset: 1 },
      done: false,
    });

    consumer.flowingCommit({ offset: 1 });
    expect(consumer.flowingCommitQueue.length).toBe(1);
    expect(consumer.flowingCommitQueue[0]).toEqual({
      message: { offset: 4 },
      done: false,
    });
  });
});
