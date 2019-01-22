package com.krux.kafka.consumer;

import com.krux.kafka.helpers.PropertiesUtils;
import com.krux.stdlib.KruxStdLib;
import com.krux.stdlib.shutdown.ShutdownTask;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class KafkaConsumer {

    private static final Logger LOG = LoggerFactory.getLogger( KafkaConsumer.class );

    private Map<String, Integer> _topicMap;
    private Map<String, List<org.apache.kafka.clients.consumer.KafkaConsumer<byte[], byte[]>>> _topicConsumers;
    private MessageHandler<?> _handler;
    Map<String, ExecutorService> _executors;
    Long _consumerPollTimeout;

    public KafkaConsumer( OptionSet options, MessageHandler<?> handler ) {

        // parse out topic->thread count mappings
        List<String> topicThreadMappings = (List<String>) options.valuesOf( "topic-threads" );
        Map<String, Integer> topicMap = new HashMap<String, Integer>();

        for ( String topicThreadCount : topicThreadMappings ) {
            if ( topicThreadCount.contains( "," ) ) {
                String[] parts = topicThreadCount.split( "," );
                topicMap.put( parts[0], Integer.parseInt( parts[1] ) );
            } else {
                topicMap.put( topicThreadCount, 1 );
            }
        }

        Properties consumerProps = (Properties) PropertiesUtils.createPropertiesFromOptionSpec( options ).clone();
        setUpConsumer( topicMap, handler, consumerProps );
    }

    public KafkaConsumer( Properties props, Map<String, Integer> topicMap, MessageHandler<?> handler ) {
        Properties consumerProps = (Properties) props.clone();
        setUpConsumer( topicMap, handler, consumerProps );
    }

    private void setUpConsumer( Map<String, Integer> topicMap, MessageHandler<?> handler, Properties consumerProps ) {
        _executors = new HashMap<String, ExecutorService>();
        _topicConsumers = new HashMap<String, List<org.apache.kafka.clients.consumer.KafkaConsumer<byte[], byte[]>>>();

        // Get consumer poll interval from CLI options
        _consumerPollTimeout = Long.parseLong(consumerProps.getProperty("consumer.poll.timeout"));
        // Remove the consumer poll interval so all properties can be passed to KafkaConsumer() clas
        consumerProps.remove("consumer.poll.timeout");
        // Required to allow an application to run both consumer and producer libraries at the same time
        consumerProps.setProperty("bootstrap.servers", consumerProps.getProperty("consumer.bootstrap.servers"));

        for ( String topic : topicMap.keySet() ) {
            String normalizedTopic = topic.replace( ".", "_" );
            String normalizedConsumerGroupId = getGroupId( consumerProps.getProperty( "group.id" ), normalizedTopic );
            consumerProps.setProperty( "group.id", normalizedConsumerGroupId );
            LOG.warn( "Consuming topic '" + topic + "' with group.id '" + normalizedConsumerGroupId + "'" );
            LOG.warn( consumerProps.toString() );
            List<org.apache.kafka.clients.consumer.KafkaConsumer<byte[], byte[]>> consumers = new ArrayList<>();
            // Read thread count for topic and create that many consumers
            for (int i = 0; i < topicMap.get(topic); i++) {
                consumers.add(new org.apache.kafka.clients.consumer.KafkaConsumer<byte[], byte[]>( consumerProps ));
            }
            _topicConsumers.put( topic, consumers);
        }
        _topicMap = topicMap;
        _handler = handler;
    }

    private String getGroupId( String groupProperty, String normalizedTopic ) {
        LOG.warn( "*****groupProperty: " + groupProperty );
        if ( groupProperty == null || groupProperty.trim().equals( "" ) || groupProperty.equals( "null" ) ) {
            try {
                LOG.warn( "*****host: " + InetAddress.getLocalHost().getHostName() );
                groupProperty = InetAddress.getLocalHost().getHostName().split( "\\." )[0].trim();
            } catch ( UnknownHostException e ) {
                LOG.error( "can't resolve hostname", e );
            }
        }
        return groupProperty + "_" + normalizedTopic;
    }

    public void start() {

        LOG.info( "Creating consumers: " );
        for ( String topic : _topicMap.keySet() ) {
            LOG.info( "Creating consumers for topic {}", topic);
            List<org.apache.kafka.clients.consumer.KafkaConsumer<byte[], byte[]>> consumers =_topicConsumers.get( topic );
            LOG.info( "consumers.size() : " + consumers.size() );

            // now create an object to consume the messages
            ExecutorService executor = Executors.newFixedThreadPool( _topicMap.get( topic ) );
            _executors.put( topic, executor );
            LOG.info( "Creating executor for topic : " + topic );
            for ( org.apache.kafka.clients.consumer.KafkaConsumer<byte[], byte[]> consumer : consumers ) {
                LOG.info( "Subscribing consumer for topic : " + topic );
                executor.submit( new ConsumerThread( consumer, _consumerPollTimeout, topic, _handler ) );
            }
        }

        KruxStdLib.registerShutdownHook( new ShutdownTask( 50 ) {
            @Override
            public void run() {
                LOG.warn( "Shutting down kafka consumer threads" );
                stop();
            }
        } );
    }

    public void stop() {
        for ( String key : _topicMap.keySet() ) {
            ExecutorService executor = _executors.get( key );
            executor.shutdownNow();
            try {
                executor.awaitTermination( 3, TimeUnit.SECONDS );
            } catch ( InterruptedException e ) {
                LOG.error( "Error waiting for consumer thread executors to terminate", e  );
            }
        }
    }

    public static void addStandardOptionsToParser( OptionParser parser ) {

        OptionSpec<String> consumerGroupName = parser.accepts( "group.id", "Consumer group name." ).withRequiredArg()
                .ofType( String.class );

        OptionSpec<String> kafkaBrokers = parser
                .accepts(
                        "consumer.bootstrap.servers",
                        "This is for bootstrapping and the producer will only use it for getting metadata (topics, partitions and replicas). The socket connections for sending the actual data will be established based on the broker information returned in the metadata. The format is host1:port1,host2:port2, and the list can be a subset of brokers or a VIP pointing to a subset of brokers." )
                .withOptionalArg().ofType( String.class ).defaultsTo( "localhost:9092" );

        OptionSpec<Long> consumerPollTimeout = parser
                .accepts(
                        "consumer.poll.timeout",
                        "The amount of time in milliseconds before timeout for each poll to the kafka servers" )
                .withOptionalArg().ofType( Long.class ).defaultsTo( 100L );

        OptionSpec<String> keyDeserializer = parser
                .accepts("key.deserializer",
                          "Deserializer class for key that implements the org.apache.kafka.common.serialization.Deserializer interface.")
                .withOptionalArg().ofType( String.class ).defaultsTo( "org.apache.kafka.common.serialization.ByteArrayDeserializer" );

        OptionSpec<String> valueDeserializer = parser
                .accepts("value.deserializer",
                        "Deserializer class for value that implements the org.apache.kafka.common.serialization.Deserializer interface.")
                .withOptionalArg().ofType( String.class ).defaultsTo( "org.apache.kafka.common.serialization.ByteArrayDeserializer" );

        OptionSpec<Integer> receiveBufferSize = parser
                .accepts( "receive.buffer.bytes", "The socket receive buffer for network requests" )
                .withRequiredArg().ofType( Integer.class ).defaultsTo( 1000 * 1024 );

        OptionSpec<Boolean> commitOffsets = parser
                .accepts(
                        "enable.auto.commit",
                        "If true, periodically commit to ZooKeeper the offset of messages already fetched by the consumer. This committed offset will be used when the process fails as the position from which the new consumer will begin." )
                .withRequiredArg().ofType( Boolean.class ).defaultsTo( Boolean.TRUE );

        OptionSpec<Integer> autoCommitInterval = parser
                .accepts( "auto.commit.interval.ms",
                        "The frequency in ms that the consumer offsets are committed to zookeeper." ).withRequiredArg()
                .ofType( Integer.class ).defaultsTo( 15 * 1000 );

        OptionSpec<Integer> fetchMinBytes = parser
                .accepts(
                        "fetch.min.bytes",
                        "The minimum amount of data the server should return for a fetch request. If insufficient data is available the request will wait for that much data to accumulate before answering the request." )
                .withRequiredArg().ofType( Integer.class ).defaultsTo( 1 );

        OptionSpec<Integer> fetchWaitMaxMs = parser
                .accepts(
                        "fetch.max.wait.ms",
                        "The maximum amount of time the server will block before answering the fetch request if there isn't sufficient data to immediately satisfy fetch.min.bytes" )
                .withRequiredArg().ofType( Integer.class ).defaultsTo( 100 );


        OptionSpec<String> autoOffsetReset = parser
                .accepts(
                        "auto.offset.reset",
                        "What to do when there is no initial offset in ZooKeeper or if an offset is out of range: * smallest : automatically reset the offset to the smallest offset * largest : automatically reset the offset to the largest offset * anything else: throw exception to the consumer" )
                .withRequiredArg().ofType( String.class ).defaultsTo( "earliest" );

        OptionSpec<String> clientId = parser
                .accepts(
                        "client.id",
                        "The client id is a user-specified string sent in each request to help trace calls. It should logically identify the application making the request." )
                .withRequiredArg().ofType( String.class ).defaultsTo( "group" );

        OptionSpec<Integer> requestTimeoutMs = parser
                .accepts(
                        "request.timeout.ms",
                        "The configuration controls the maximum amount of time the client will wait for the response of a request. If the response is not received before the timeout elapses the client will resend the request if necessary or fail the request if retries are exhausted." )
                .withRequiredArg().ofType( Integer.class ).defaultsTo( 10000 );

        OptionSpec<Integer> sessionTimeoutMs = parser
                .accepts(
                        "session.timeout.ms",
                        "ZooKeeper session timeout. If the consumer fails to heartbeat to ZooKeeper for this period of time it is considered dead and a rebalance will occur." )
                .withRequiredArg().ofType( Integer.class ).defaultsTo( 6000 );

        OptionSpec<String> topicThreadMapping = parser
                .accepts(
                        "topic-threads",
                        "Topic and number of threads to listen to that topic.  Example: '--topic.threads topic1,4' "
                                + "would configure 4 threads to consumer messages from the 'topic1' topic.  Multiple topics can be configured "
                                + "by passing multiple cl options, e.g.: '--topic.threads topic1,4 --topic.threads topic2,8'. At least"
                                + "one --topic.thread must be specified.  The thread pool sizes can be omitted, like so: '--topic.threads topic1 "
                                + "--topic.threads topic2' If so, each topic will be assigned a single thread for consumption." )
                .withRequiredArg().ofType( String.class );

    }

    public static OptionParser getStandardOptionParser() {

        OptionParser parser = new OptionParser();

        addStandardOptionsToParser( parser );

        return parser;
    }

}
