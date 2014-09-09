package com.krux.kafka.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.krux.stdlib.KruxStdLib;

public class KafkaConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumer.class);

    private final Map<String, Integer> _topicMap;
    private final Map<String, ConsumerConnector> _topicConsumers;
    private final MessageHandler<byte[]> _handler;

    public KafkaConsumer(ConsumerConfig config, Map<String, Integer> topicMap, MessageHandler<byte[]> handler) {

        _topicConsumers = new HashMap<String, ConsumerConnector>();

        for (String topic : topicMap.keySet()) {
            ConsumerConfig topicConfig = new ConsumerConfig(config.props().props());
            _topicConsumers.put(topic, kafka.consumer.Consumer.createJavaConsumerConnector(topicConfig));
        }
        _topicMap = topicMap;
        _handler = handler;
    }

    public void start() {
        // Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        // String topic = _config.props().getProperty("topic");
        // LOG.info("topic: " + topic);
        // int numOfThreads =
        // Integer.parseInt(_config.props().getProperty("num.of.threads"));
        // LOG.info("numOfThreads: " + numOfThreads);
        // topicCountMap.put(topic, numOfThreads);

        final Map<String, ExecutorService> executors = new HashMap<String, ExecutorService>();
        for (String topic : _topicMap.keySet()) {

            Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = _topicConsumers.get(topic).createMessageStreams(
                    _topicMap);

            List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

            // now create an object to consume the messages
            for (final KafkaStream<byte[], byte[]> stream : streams) {
                ExecutorService executor = Executors.newFixedThreadPool(_topicMap.get(topic));
                executors.put(stream.toString(), executor);
                executor.submit(new ConsumerThread(stream, topic, _handler));
            }
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                for (String key : _topicMap.keySet()) {
                    ExecutorService executor = executors.get(key);
                    executor.shutdownNow();
                }
            }
        });
    }

//    static public void main(String[] args) {
//        OptionParser parser = new OptionParser();
//
//        Map<String, OptionSpec> optionSpecs = new HashMap<String, OptionSpec>();
//
//        // expose all kafka consumer config params to the cli
//
//        OptionSpec<String> consumerGroupName = parser.accepts("group.id", "Consumer group name.").withRequiredArg()
//                .ofType(String.class);
//        optionSpecs.put("group.id", consumerGroupName);
//
//        OptionSpec<String> zookeeperUrl = parser
//                .accepts(
//                        "zookeeper.connect",
//                        "a connection string containing a comma separated list of host:port pairs, each corresponding to a "
//                                + "ZooKeeper server (e.g. \"127.0.0.1:4545\" or \"127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002\"). "
//                                + "An optional \"chroot\" suffix may also be appended to the connection string. (e.g. "
//                                + "\"127.0.0.1:4545/app/a\" or \"127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002/app/a\"). ")
//                .withRequiredArg().ofType(String.class);
//        optionSpecs.put("zookeeper.connect", zookeeperUrl);
//
//        OptionSpec<Integer> socketTimeout = parser
//                .accepts("socket.timeout.ms",
//                        "The socket timeout for network requests. The actual timeout set will be max.fetch.wait + socket.timeout.ms.")
//                .withRequiredArg().ofType(Integer.class).defaultsTo(30 * 1000);
//        optionSpecs.put("socket.timeout.ms", socketTimeout);
//
//        OptionSpec<Integer> receiveBufferSize = parser
//                .accepts("socket.receive.buffer.bytes", "The socket receive buffer for network requests").withRequiredArg()
//                .ofType(Integer.class).defaultsTo(64 * 1024);
//        optionSpecs.put("socket.receive.buffer.bytes", receiveBufferSize);
//
//        OptionSpec<Boolean> parseMessagesAsJson = parser
//                .accepts("parse.messages.as.json", "Toggle for parsing messages as JSON").withRequiredArg()
//                .ofType(Boolean.class).defaultsTo(Boolean.FALSE);
//        optionSpecs.put("parse.messages.as.json", parseMessagesAsJson);
//
//        OptionSpec<Integer> messageMaxSize = parser
//                .accepts(
//                        "fetch.message.max.bytes",
//                        "The number of byes of messages to attempt to fetch for each topic-partition in each fetch request. These bytes will be read into memory for each partition, so this helps control the memory used by the consumer. The fetch request size must be at least as large as the maximum message size the server allows or else it is possible for the producer to send messages larger than the consumer can fetch.")
//                .withRequiredArg().ofType(Integer.class).defaultsTo(1024 * 1024);
//        optionSpecs.put("fetch.message.max.bytes", messageMaxSize);
//
//        OptionSpec<Boolean> commitOffsets = parser
//                .accepts(
//                        "auto.commit.enable",
//                        "If true, periodically commit to ZooKeeper the offset of messages already fetched by the consumer. This committed offset will be used when the process fails as the position from which the new consumer will begin.")
//                .withRequiredArg().ofType(Boolean.class).defaultsTo(Boolean.TRUE);
//        optionSpecs.put("auto.commit.enable", commitOffsets);
//
//        OptionSpec<Integer> autoCommitInterval = parser
//                .accepts("auto.commit.interval.ms",
//                        "The frequency in ms that the consumer offsets are committed to zookeeper.").withRequiredArg()
//                .ofType(Integer.class).defaultsTo(60 * 1000);
//        optionSpecs.put("auto.commit.interval.ms", autoCommitInterval);
//
//        OptionSpec<Integer> maxMessageChunks = parser
//                .accepts("queued.max.message.chunks",
//                        "Max number of message chunks buffered for consumption. Each chunk can be up to fetch.message.max.bytes.")
//                .withRequiredArg().ofType(Integer.class).defaultsTo(100);
//        optionSpecs.put("queued.max.message.chunks", maxMessageChunks);
//
//        OptionSpec<Integer> rebalanceMaxRetries = parser
//                .accepts(
//                        "rebalance.max.retries",
//                        "When a new consumer joins a consumer group the set of consumers attempt to \"rebalance\" the load to assign partitions to each consumer. If the set of consumers changes while this assignment is taking place the rebalance will fail and retry. This setting controls the maximum number of attempts before giving up.")
//                .withRequiredArg().ofType(Integer.class).defaultsTo(4);
//        optionSpecs.put("rebalance.max.retries", rebalanceMaxRetries);
//
//        OptionSpec<Integer> fetchMinBytes = parser
//                .accepts(
//                        "fetch.min.bytes",
//                        "The minimum amount of data the server should return for a fetch request. If insufficient data is available the request will wait for that much data to accumulate before answering the request.")
//                .withRequiredArg().ofType(Integer.class).defaultsTo(1);
//        optionSpecs.put("fetch.min.bytes", fetchMinBytes);
//
//        OptionSpec<Integer> fetchWaitMaxMs = parser
//                .accepts(
//                        "fetch.wait.max.ms",
//                        "The maximum amount of time the server will block before answering the fetch request if there isn't sufficient data to immediately satisfy fetch.min.bytes")
//                .withRequiredArg().ofType(Integer.class).defaultsTo(100);
//        optionSpecs.put("fetch.wait.max.ms", fetchWaitMaxMs);
//
//        OptionSpec<Integer> rebalanceBackoffMs = parser
//                .accepts("rebalance.backoff.ms", "Backoff time between retries during rebalance.").withRequiredArg()
//                .ofType(Integer.class).defaultsTo(2000);
//        optionSpecs.put("rebalance.backoff.ms", rebalanceBackoffMs);
//
//        OptionSpec<Integer> refreshLeaderBackoff = parser
//                .accepts("refresh.leader.backoff.ms",
//                        "Backoff time to wait before trying to determine the leader of a partition that has just lost its leader.")
//                .withRequiredArg().ofType(Integer.class).defaultsTo(200);
//        optionSpecs.put("refresh.leader.backoff.ms", refreshLeaderBackoff);
//
//        OptionSpec<String> autoOffsetReset = parser
//                .accepts(
//                        "auto.offset.reset",
//                        "What to do when there is no initial offset in ZooKeeper or if an offset is out of range: * smallest : automatically reset the offset to the smallest offset * largest : automatically reset the offset to the largest offset * anything else: throw exception to the consumer")
//                .withRequiredArg().ofType(String.class).defaultsTo("largest");
//        optionSpecs.put("auto.offset.reset", autoOffsetReset);
//
//        OptionSpec<String> messageLogDir = parser
//                .accepts("message.log.dir", "Absolute path to directory under which files will be logged.")
//                .withRequiredArg().ofType(String.class);
//
//        OptionSpec<Integer> consumerTimeoutMs = parser
//                .accepts("consumer.timeout.ms",
//                        "Throw a timeout exception to the consumer if no message is available for consumption after the specified interval")
//                .withRequiredArg().ofType(Integer.class).defaultsTo(60 * 60 * 1000);
//        optionSpecs.put("consumer.timeout.ms", consumerTimeoutMs);
//
//        OptionSpec<String> clientId = parser
//                .accepts(
//                        "client.id",
//                        "The client id is a user-specified string sent in each request to help trace calls. It should logically identify the application making the request.")
//                .withRequiredArg().ofType(String.class).defaultsTo("group");
//        optionSpecs.put("client.id", clientId);
//
//        OptionSpec<Integer> zookeeperSessionTimeoutMs = parser
//                .accepts(
//                        "zookeeper.session.timeout.ms",
//                        "ZooKeeper session timeout. If the consumer fails to heartbeat to ZooKeeper for this period of time it is considered dead and a rebalance will occur.")
//                .withRequiredArg().ofType(Integer.class).defaultsTo(6000);
//        optionSpecs.put("zookeeper.session.timeout.ms", zookeeperSessionTimeoutMs);
//
//        OptionSpec<Integer> zookeeperConnectionTimeoutMs = parser
//                .accepts("zookeeper.connection.timeout.ms",
//                        "The max time that the client waits while establishing a connection to zookeeper.")
//                .withRequiredArg().ofType(Integer.class).defaultsTo(6000);
//        optionSpecs.put("zookeeper.connection.timeout.ms", zookeeperConnectionTimeoutMs);
//
//        OptionSpec<Integer> zookeeperSyncTimeMs = parser
//                .accepts("zookeeper.sync.time.ms", "How far a ZK follower can be behind a ZK leader").withRequiredArg()
//                .ofType(Integer.class).defaultsTo(2000);
//        optionSpecs.put("zookeeper.sync.time.ms", zookeeperSyncTimeMs);
//
//        OptionSpec<Integer> fileRotationInterval = parser
//                .accepts(
//                        "file.rotation.interval.mins",
//                        "Number of minutes between message log file rotations. Rotations happen on "
//                                + "even divisions of this number. For example, specifying '10' as this value will cause files to be "
//                                + "rotated at 12:00, 12:10, 12:20, etc.  Specifying '5' will cause files to be rotated at 12:00, 12:05, "
//                                + "12:10, etc.  All topic log files will be rotated on the same schedule.")
//                .withRequiredArg().ofType(Integer.class).defaultsTo(10);
//        optionSpecs.put("file.rotation.interval", zookeeperSyncTimeMs);
//
//        OptionSpec<String> topicThreadMapping = parser
//                .accepts(
//                        "topic-threads",
//                        "Topic and number of threads to listen to that topic.  Example: '--topic.threads topic1,4' "
//                                + "would configure 4 threads to consumer messages from the 'topic1' topic.  Multiple topics can be configured "
//                                + "by passing multiple cl options, e.g.: '--topic.threads topic1,4 --topic.threads topic2,8'. At least"
//                                + "one --topic.thread must be specified.  The thread pool sizes can be omitted, like so: '--topic.threads topic1 "
//                                + "--topic.threads topic2' If so, each topic will be assigned a single thread for consumption.")
//                .withRequiredArg().ofType(String.class);
//        optionSpecs.put("topic.threads", topicThreadMapping);
//
//        KruxStdLib.setOptionParser(parser);
//        OptionSet options = KruxStdLib.initialize(args);
//
//        // ensure required cl options are present
//        if (!options.has(topicThreadMapping) || !options.has(consumerGroupName) || !options.has(zookeeperUrl)
//                || !options.has(messageLogDir)) {
//            LOG.error("'--topic-threads', '--group.id', '--message.log.dir' and '--zookeeper.connect' and all required parameters. Exitting!");
//            System.exit(-1);
//        }
//
//        // parse out topic->thread count mappings
//        List<String> topicThreadMappings = options.valuesOf(topicThreadMapping);
//        Map<String, Integer> topicMap = new HashMap<String, Integer>();
//
//        for (String topicThreadCount : topicThreadMappings) {
//            if (topicThreadCount.contains(",")) {
//                String[] parts = topicThreadCount.split(",");
//                topicMap.put(parts[0], Integer.parseInt(parts[1]));
//            } else {
//                topicMap.put(topicThreadCount, 1);
//            }
//        }
//
//        // create single ConsumerConfig for all mappings. Topic and thread
//        // counts will be overridden in BeaconStreamLogger
//        ConsumerConfig config = createConsumerConfig(options, optionSpecs);
//
//        KafkaConsumer runner = new KafkaConsumer(config, topicMap);
//        runner.run();
//    }

    @SuppressWarnings("unchecked")
    public static ConsumerConfig createConsumerConfig(OptionSet options, Map<String, OptionSpec> optionSpecs) {
        Properties props = new Properties();

        for (String key : optionSpecs.keySet()) {
            LOG.info(key + ": " + options.valueOf(optionSpecs.get(key)));
            props.put(key, String.valueOf(options.valueOf(optionSpecs.get(key))));
        }
        return new ConsumerConfig(props);
    }
}
