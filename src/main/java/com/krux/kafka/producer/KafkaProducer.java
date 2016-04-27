package com.krux.kafka.producer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.krux.kafka.helpers.PropertiesUtils;
import com.krux.stdlib.KruxStdLib;
import com.krux.stdlib.shutdown.ShutdownTask;

public class KafkaProducer {

    private static final Logger LOG = LoggerFactory.getLogger( KafkaProducer.class );

    private final Producer<byte[], byte[]> _producer;
    private final String _topic;

    private static final List<Producer> _producers = Collections.synchronizedList( new ArrayList<Producer>() );

    static {
        KruxStdLib.registerShutdownHook( new ShutdownTask( 100 ) {
            @Override
            public void run() {
                LOG.info( "Shutting down kafka producers" );
                for ( Producer producer : _producers ) {
                    producer.close();
                }
            }
        } );
    }
    
    //assumes defaults
//    public KafkaProducer( String topic ) {
//        Properties props = getDefaultProps();
//        props.setProperty( "topic", topic );
//        ProducerConfig config = new ProducerConfig( props );
//        _producer = new Producer<byte[], byte[]>( config );
//        _producers.add( _producer );
//        _topic = topic;
//    }

    private Properties getDefaultProps() {
        OptionParser tempParser = getStandardOptionParser();
        String[] args = new String[0];
        OptionSet options = tempParser.parse( args );
        return PropertiesUtils.createPropertiesFromOptionSpec( options );
    }

    public KafkaProducer( Properties props, String topic ) {
        LOG.warn( "Producer properties: " + props.toString() );
        ProducerConfig config = new ProducerConfig( props );
        _producer = new Producer<byte[], byte[]>( config );
        _producers.add( _producer );
        _topic = topic;
    }

    public KafkaProducer( OptionSet options, String topic ) {
        Properties props = PropertiesUtils.createPropertiesFromOptionSpec( options );
        LOG.warn( "Producer properties: " + props.toString() );
        // producerProps.put("partitioner.class",
        // System.getProperty("partitioner.class",
        // "com.krux.kafka.producer.SimplePartitioner"));
        ProducerConfig config = new ProducerConfig( props );
        _producer = new Producer<byte[], byte[]>( config );
        _producers.add( _producer );
        _topic = topic;
    }

    public void send( String message ) {
        send( "", message );
    }

    public void send( String key, String message ) {
        send( key.getBytes(), message.getBytes() );
    }

    public void send( byte[] message ) {
        send( "".getBytes(), message );
    }

    /**
     * The send is asynchronous and this method will return immediately once the record
     * has been stored in the buffer of records waiting to be sent.
     * This allows sending many records in parallel without blocking to wait
     * for the response after each one.
     *
     * @param key key
     * @param message value
     */
    public void send( byte[] key, byte[] message ) {
        long start = System.currentTimeMillis();
        LOG.debug( "Sending message to {}", _topic );
        KeyedMessage<byte[], byte[]> data = new KeyedMessage<byte[], byte[]>( _topic, key, message );
        _producer.send( data );
        long time = System.currentTimeMillis() - start;
        try {
            if ( KruxStdLib.STATSD == null ) {
                LOG.error( "WTF? STATSD is null?!" );
            }
            KruxStdLib.STATSD.time( "message_sent." + _topic, time );
            KruxStdLib.STATSD.time( "message_sent_all", time );
        } catch ( Exception e ) {
            LOG.error( "cannot send statsd stats", e );
        }
    }

    public static void addStandardOptionsToParser( OptionParser parser ) {
        OptionSpec<String> topic = parser.accepts( "topic", "The topic to which messages will be sent" ).withRequiredArg()
                .ofType( String.class );
        OptionSpec<String> kafkaBrokers = parser
                .accepts(
                        "metadata.broker.list",
                        "This is for bootstrapping and the producer will only use it for getting metadata (topics, partitions and replicas). The socket connections for sending the actual data will be established based on the broker information returned in the metadata. The format is host1:port1,host2:port2, and the list can be a subset of brokers or a VIP pointing to a subset of brokers." )
                .withOptionalArg().ofType( String.class ).defaultsTo( "localhost:9092" );
        OptionSpec<Integer> kafkaAckType = parser
                .accepts(
                        "request.required.acks",
                        "The type of ack the broker will return to the client.\n  0, which means that the producer never waits for an acknowledgement\n  1, which means that the producer gets an acknowledgement after the leader replica has received the data.\n  -1, which means that the producer gets an acknowledgement after all in-sync replicas have received the data.\nSee https://kafka.apache.org/documentation.html#producerconfigs" )
                .withOptionalArg().ofType( Integer.class ).defaultsTo( 1 );

        OptionSpec<String> producerType = parser.accepts( "producer.type", "'sync' or 'async'" ).withOptionalArg()
                .ofType( String.class ).defaultsTo( "async" );

        OptionSpec<Integer> kafkaRequestTimeoutMs = parser
                .accepts(
                        "request.timeout.ms",
                        "The amount of time the broker will wait trying to meet the request.required.acks requirement before sending back an error to the client." )
                .withOptionalArg().ofType( Integer.class ).defaultsTo( 10000 );
        OptionSpec<String> kafkaCompressionType = parser
                .accepts(
                        "compression.codec",
                        "This parameter allows you to specify the compression codec for all data generated by this producer. Valid values are \"none\", \"gzip\" and \"snappy\"." )
                .withOptionalArg().ofType( String.class ).defaultsTo( "none" );
        OptionSpec<Integer> messageSendMaxRetries = parser
                .accepts(
                        "message.send.max.retries",
                        "This property will cause the producer to automatically retry a failed send request. This property specifies the number of retries when such failures occur. Note that setting a non-zero value here can lead to duplicates in the case of network errors that cause a message to be sent but the acknowledgement to be lost." )
                .withOptionalArg().ofType( Integer.class ).defaultsTo( 3 );
        OptionSpec<Integer> retryBackoffMs = parser
                .accepts(
                        "retry.backoff.ms",
                        "Before each retry, the producer refreshes the metadata of relevant topics to see if a new leader has been elected. Since leader election takes a bit of time, this property specifies the amount of time that the producer waits before refreshing the metadata." )
                .withOptionalArg().ofType( Integer.class ).defaultsTo( 100 );
        OptionSpec<Integer> queueBufferingMaxMs = parser
                .accepts(
                        "queue.buffering.max.ms",
                        "Maximum time to buffer data when using async mode. For example a setting of 100 will try to batch together 100ms of messages to send at once. This will improve throughput but adds message delivery latency due to the buffering." )
                .withOptionalArg().ofType( Integer.class ).defaultsTo( 500 );
        OptionSpec<Integer> queueBufferingMaxMessages = parser
                .accepts(
                        "queue.buffering.max.messages",
                        "The maximum number of unsent messages that can be queued up the producer when using async mode before either the producer must be blocked or data must be dropped." )
                .withOptionalArg().ofType( Integer.class ).defaultsTo( 10000 );
        OptionSpec<Integer> queueEnqueTimeoutMs = parser
                .accepts(
                        "queue.enqueue.timeout.ms",
                        "The amount of time to block before dropping messages when running in async mode and the buffer has reached queue.buffering.max.messages. If set to 0 events will be enqueued immediately or dropped if the queue is full (the producer send call will never block). If set to -1 the producer will block indefinitely and never willingly drop a send." )
                .withOptionalArg().ofType( Integer.class ).defaultsTo( -1 );
        OptionSpec<Integer> batchNumMessages = parser
                .accepts(
                        "batch.num.messages",
                        "The number of messages to send in one batch when using async mode. The producer will wait until either this number of messages are ready to send or queue.buffer.max.ms is reached." )
                .withOptionalArg().ofType( Integer.class ).defaultsTo( 2000 );
        OptionSpec<String> clientId = parser
                .accepts(
                        "client.id",
                        "The client id is a user-specified string sent in each request to help trace calls. It should logically identify the application making the request." )
                .withOptionalArg().ofType( String.class ).defaultsTo( "" );
        OptionSpec<Integer> sendBufferBytes = parser.accepts( "send.buffer.bytes", "Socket write buffer size" )
                .withOptionalArg().ofType( Integer.class ).defaultsTo( 100 * 1024 );
        OptionSpec<String> partitionClass = parser
                .accepts( "partitioner.class", "The partitioner class for partitioning messages amongst sub-topics. " )
                .withOptionalArg().ofType( String.class ).defaultsTo( "com.krux.kafka.producer.SimplePartitioner" );

    }

    public static OptionParser getStandardOptionParser() {

        OptionParser parser = new OptionParser();
        addStandardOptionsToParser( parser );

        return parser;
    }

    /**
     * Close all the producers. This method blocks until all in-flight requests complete.
     */
    public static void shutdownAndCloseAll() {
        LOG.info( "Shutting down kafka producers" );
        for (Producer producer : _producers) {
            producer.close();
        }
    }

    /**
     * Close this producer. This method blocks until all in-flight requests complete.
     */
    public void close() {
        _producer.close();
    }
    
}