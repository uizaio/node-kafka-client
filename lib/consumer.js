const assert = require("assert");
const { KafkaConsumer } = require("node-rdkafka");

/**
 * @param {Object} options
 * @param {String} options.name
 * @param {String} options.host
 * @param {String} options.groupId
 * @param {String} options.topic
 * @param {"flowing" | "non-flowing"} [options.mode] "flowing || non-flowing". Default: "non-flowing"
 * @param {Object} [options.logger]
 * @param {Number} [options.connectTimeout] in millisecond. Default: 5000 ms
 * @param {Number} [options.intervalFetchMessage] Default: 10 ms
 * @param {Number} [options.numMsgFetchPerTime] Default: 1
 * @param {*} [options.rdKafkaConfig]
 * @param {*} [options.rdKafkaTopicConfig]
 */
function Consumer(options = {}) {
  assert(typeof options === "object", new Error("New consumer config is required"));

  this.options = {
    name: options.name,
    host: options.host,
    groupId: options.groupId,
    topic: options.topic,
    logger: options.logger,
    mode: options.mode || "non-flowing",
    connectTimeout: options.connectTimeout || 5000,
    numMsgFetchPerTime: options.numMsgFetchPerTime || 1,
    intervalFetchMessage: options.intervalFetchMessage || 10,
    rdKafkaConfig: options.rdKafkaConfig,
    rdKafkaTopicConfig: options.rdKafkaTopicConfig,
  };

  assert(this.options.name, new Error("Option 'name' is required"));
  assert(this.options.host, new Error("Option 'host' is required"));
  assert(this.options.groupId, new Error("Option 'groupId' is required"));
  assert(this.options.topic, new Error("Option 'topic' is required"));
  assert(
    ["flowing", "non-flowing"].includes(this.options.mode),
    new Error("Option 'mode' should be set 'flowing' or 'non-flowing'"),
  );

  assert(Number.isInteger(this.options.connectTimeout), new Error("Option 'connectTimeout' should be integer"));
  assert(this.options.connectTimeout > 0, new Error("Option 'connectTimeout' should greater than 0"));
  assert(
    Number.isInteger(this.options.intervalFetchMessage),
    new Error("Option 'intervalFetchMessage' should be integer"),
  );
  assert(this.options.intervalFetchMessage > 0, new Error("Option 'intervalFetchMessage' should greater than 0"));
  assert(Number.isInteger(this.options.numMsgFetchPerTime), new Error("Option 'numMsgFetchPerTime' should be integer"));
  assert(this.options.numMsgFetchPerTime > 0, new Error("Option 'numMsgFetchPerTime' should greater than 0"));

  this.logger = this.options.logger;

  this.consumer = new KafkaConsumer(
    {
      "group.id": this.options.groupId,
      "metadata.broker.list": this.options.host,
      "enable.auto.commit": false,
      "socket.keepalive.enable": true,
      event_cb: true,
      ...(this.options.rdKafkaConfig || {}),
    },
    {
      "auto.offset.reset": "earliest",
      ...(this.options.rdKafkaTopicConfig || {}),
    },
  );
}

/**
 * This is use for external adding
 * event listener to rd-kafka consumer
 */
Consumer.prototype.on = function on(event, handler) {
  this.consumer.on(event, handler);
};

/**
 * Main entry function to start consumer
 */
Consumer.prototype.listen = function listen(handler) {
  this.debug("Start consumer with options");

  this.consumer.connect({ timeout: this.options.connectTimeout }, (err) => {
    if (err) {
      throw new Error(`Cannot connect to Kafka.\n${err.stack}`);
    }
  });

  this.consumer.on("event.error", (error) => {
    if (error.stack.includes("Local: Broker transport failure")) throw error;
    this.warn(error);
  });

  this.consumer.once("ready", () => {
    const { topic, name, mode } = this.options;
    this.consumer.subscribe([topic]);
    this.debug(`Consumer ${name} is ready on topic ${topic}`);

    if (mode === "flowing") this.flowingConsume(handler);
    else this.nonFlowingConsume(handler);
  });
};

/**
 * Recursive function consume message
 */
Consumer.prototype.nonFlowingConsume = function nonFlowingConsume(handler) {
  this.consumer.consume(this.options.numMsgFetchPerTime, async (err, messages) => {
    if (err) throw err;

    if (messages.length) {
      await this.nonFlowingCastMessages(messages, handler);
    }

    // Take a delay before consume next messages
    await new Promise((resolve) => {
      setTimeout(resolve, this.options.intervalFetchMessage);
    });

    await this.nonFlowingConsume(handler);
  });
};

/**
 * Cast each messages which is consumed from non flowing mode
 * to the handler
 *
 * This function is recursive
 *
 * @param {Array<*>} messages
 */
Consumer.prototype.nonFlowingCastMessages = async function nonFlowingCastMessages(messages, handler) {
  const msg = messages.shift();
  await handler(msg);
  this.consumer.commitMessageSync(msg);
  this.debug(`Committed topic ${msg.topic} partition ${msg.partition} offset ${msg.offset}`);

  // Adding timer to make this function
  // not blocking the event loop
  await new Promise((resolve) => {
    setImmediate(resolve);
  });

  // Recursive cast next message to handler
  if (messages.length) await this.nonFlowingCastMessages(messages, handler);
};

/**
 * Consume message as flowing mode
 */
Consumer.prototype.flowingConsume = function flowingConsume(handler) {
  this.flowingCommitQueue = [];

  this.consumer.on("data", async (message) => {
    this.flowingCommitQueue.push({ message, done: false });
    await handler(message);
    this.flowingCommit(message);
  });

  this.consumer.consume();
};

/**
 * Commit message in-order
 * for flowing mode
 */
Consumer.prototype.flowingCommit = function flowingCommit(message) {
  const index = this.flowingCommitQueue.findIndex((m) => m.message.offset === message.offset);

  this.flowingCommitQueue[index].done = true;

  this.debug(`Flowing commit queue: topic ${message.topic} partition ${message.partition} offset ${message.offset}`);

  while (this.flowingCommitQueue.length && this.flowingCommitQueue[0].done === true) {
    const msgToCommit = this.flowingCommitQueue.shift().message;
    this.consumer.commitMessageSync(msgToCommit);
    this.debug(`Committed topic ${msgToCommit.topic} partition ${msgToCommit.partition} offset ${msgToCommit.offset}`);
  }
};

/**
 * Log debug message if logger is set
 */
Consumer.prototype.debug = function debug(log) {
  // eslint-disable-next-line no-unused-expressions
  this.logger && this.logger.debug(log);
};

/**
 * Log warning message if logger is set
 */
Consumer.prototype.warn = function warn(log) {
  // eslint-disable-next-line no-unused-expressions
  this.logger && this.logger.warn(log);
};

module.exports.Consumer = Consumer;
