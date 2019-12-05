package com.krux.kafka.producer;

import com.krux.kafka.helpers.PropertiesUtils;
import com.krux.stdlib.KruxStdLib;
import com.krux.stdlib.shutdown.ShutdownTask;
import com.krux.stdlib.statsd.StatsdClient;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import kafka.api.ProducerResponseStatus;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaProducer {

    private static final Logger LOG = LoggerFactory.getLogger( KafkaProducer.class );

    private final org.apache.kafka.clients.producer.KafkaProducer<byte[], byte[]> _producer;
    private final String _topic;
    private final StatsdClient _statsd;

    private static final List<org.apache.kafka.clients.producer.KafkaProducer> _producers = Collections.synchronizedList( new ArrayList<org.apache.kafka.clients.producer.KafkaProducer>() );
    private static volatile boolean initialized = false;

    private void initShutdownHook() {
        if (!initialized) {
            KruxStdLib.get().registerShutdownHook( new ShutdownTask( 100 ) {
                @Override
                public void run() {
                    LOG.info( "Shutting down kafka producers" );
                    for ( org.apache.kafka.clients.producer.KafkaProducer producer : _producers ) {
                        producer.close();
                    }
                }
            } );
            initialized = true;
        }
    }

    private Properties getDefaultProps() {
        OptionParser tempParser = getStandardOptionParser();
        String[] args = new String[0];
        OptionSet options = tempParser.parse( args );
        return PropertiesUtils.createPropertiesFromOptionSpec( options );
    }

    public KafkaProducer( Properties props, String topic ) {
        this(props, topic, KruxStdLib.get().getStatsdClient());
    }

    public KafkaProducer( Properties props, String topic, StatsdClient statsd ) {
        LOG.warn( "Producer properties: " + props.toString() );
        // Required to allow an application to run both consumer and producer libraries at the same time
        props.setProperty("bootstrap.servers", props.getProperty("producer.bootstrap.servers"));
        _producer = new org.apache.kafka.clients.producer.KafkaProducer<byte[], byte[]>( props );
        _producers.add( _producer );
        _topic = topic;
        _statsd = statsd;
        initShutdownHook();
    }

    public KafkaProducer( OptionSet options, String topic ) {
        this(options, topic, KruxStdLib.get().getStatsdClient());
    }

    public KafkaProducer( OptionSet options, String topic, StatsdClient statsd ) {
        Properties props = PropertiesUtils.createPropertiesFromOptionSpec( options );
        // Required to allow an application to run both consumer and producer libraries at the same time
        props.setProperty("bootstrap.servers", props.getProperty("producer.bootstrap.servers"));
        LOG.warn( "Producer properties: " + props.toString() );
        _producer = new org.apache.kafka.clients.producer.KafkaProducer<byte[], byte[]>( props );
        _producers.add( _producer );
        _topic = topic;
        _statsd = statsd;
        initShutdownHook();
    }

    public Future<RecordMetadata> send( String message ) {
        return send( "", message );
    }

    public Future<RecordMetadata> send( String key, String message ) {
        return send( key.getBytes(), message.getBytes() );
    }

    public Future<RecordMetadata> send( byte[] message ) {
        return send( "".getBytes(), message );
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
    public Future<RecordMetadata> send( byte[] key, byte[] message ) {
        long start = System.currentTimeMillis();
        LOG.debug( "Sending message to {}", _topic );
        ProducerRecord<byte[], byte[]> data = new ProducerRecord<byte[], byte[]>( _topic, key.length == 0 ? null : key, message );
        Future<RecordMetadata> response = _producer.send( data );
        long time = System.currentTimeMillis() - start;
        try {
            _statsd.time( "message_sent." + _topic, time );
            _statsd.time( "message_sent_all", time );
        } catch ( Exception e ) {
            LOG.error( "cannot send statsd stats", e );
        }

        return response;
    }

    public static void addStandardOptionsToParser( OptionParser parser ) {
        OptionSpec<String> topic = parser.accepts( "topic", "The topic to which messages will be sent" ).withRequiredArg()
                .ofType( String.class );

        OptionSpec<String> keySerializer = parser
                .accepts("key.serializer",
                        "Serializer class for key that implements the org.apache.kafka.common.serialization.Serializer interface.")
                .withOptionalArg().ofType( String.class ).defaultsTo( "org.apache.kafka.common.serialization.ByteArraySerializer" );

        OptionSpec<String> valueSerializer = parser
                .accepts("value.serializer",
                        "Serializer class for value that implements the org.apache.kafka.common.serialization.Serializer interface.")
                .withOptionalArg().ofType( String.class ).defaultsTo( "org.apache.kafka.common.serialization.ByteArraySerializer" );

        OptionSpec<String> kafkaBrokers = parser
                .accepts(
                        "producer.bootstrap.servers",
                        "A list of host/port pairs to use for establishing the initial connection to the Kafka cluster. The client will make use of all servers irrespective of which servers are specified here for bootstrapping—this list only impacts the initial hosts used to discover the full set of servers. This list should be in the form host1:port1,host2:port2,.... Since these servers are just used for the initial connection to discover the full cluster membership (which may change dynamically), this list need not contain the full set of servers (you may want more than one, though, in case a server is down)." )
                .withOptionalArg().ofType( String.class ).defaultsTo( "localhost:9092" );
        OptionSpec<Integer> kafkaAckType = parser
                .accepts(
                        "acks",
                        "The number of acknowledgments the producer requires the leader to have received before considering a request complete. This controls the durability of records that are sent. The following settings are allowed:\n" +
                                "acks=0 If set to zero then the producer will not wait for any acknowledgment from the server at all. The record will be immediately added to the socket buffer and considered sent. No guarantee can be made that the server has received the record in this case, and the retries configuration will not take effect (as the client won't generally know of any failures). The offset given back for each record will always be set to -1.\n" +
                                "acks=1 This will mean the leader will write the record to its local log but will respond without awaiting full acknowledgement from all followers. In this case should the leader fail immediately after acknowledging the record but before the followers have replicated it then the record will be lost.\n" +
                                "acks=all This means the leader will wait for the full set of in-sync replicas to acknowledge the record. This guarantees that the record will not be lost as long as at least one in-sync replica remains alive. This is the strongest available guarantee. This is equivalent to the acks=-1 setting." )
                .withOptionalArg().ofType( Integer.class ).defaultsTo( 1 );
        OptionSpec<Integer> kafkaRequestTimeoutMs = parser
                .accepts(
                        "request.timeout.ms",
                        "The amount of time the broker will wait trying to meet the request.required.acks requirement before sending back an error to the client." )
                .withOptionalArg().ofType( Integer.class ).defaultsTo( 10000 );
        OptionSpec<String> kafkaCompressionType = parser
                .accepts(
                        "compression.type",
                        "The compression type for all data generated by the producer. The default is none (i.e. no compression). Valid values are none, gzip, snappy, lz4, or zstd. Compression is of full batches of data, so the efficacy of batching will also impact the compression ratio (more batching means better compression)." )
                .withOptionalArg().ofType( String.class ).defaultsTo( "none" );
        OptionSpec<Integer> messageSendMaxRetries = parser
                .accepts(
                        "retries",
                        "This property will cause the producer to automatically retry a failed send request. This property specifies the number of retries when such failures occur. Note that setting a non-zero value here can lead to duplicates in the case of network errors that cause a message to be sent but the acknowledgement to be lost." )
                .withOptionalArg().ofType( Integer.class ).defaultsTo( 3 );
        OptionSpec<Integer> retryBackoffMs = parser
                .accepts(
                        "retry.backoff.ms",
                        "Before each retry, the producer refreshes the metadata of relevant topics to see if a new leader has been elected. Since leader election takes a bit of time, this property specifies the amount of time that the producer waits before refreshing the metadata." )
                .withOptionalArg().ofType( Integer.class ).defaultsTo( 100 );
        OptionSpec<Integer> batchNumMessages = parser
                .accepts(
                        "batch.size",
                        "The producer will attempt to batch records together into fewer requests whenever multiple records are being sent to the same partition. This helps performance on both the client and the server. This configuration controls the default batch size in bytes.\n" +
                                "No attempt will be made to batch records larger than this size.\n" +
                                "\n" +
                                "Requests sent to brokers will contain multiple batches, one for each partition with data available to be sent.\n" +
                                "\n" +
                                "A small batch size will make batching less common and may reduce throughput (a batch size of zero will disable batching entirely). A very large batch size may use memory a bit more wastefully as we will always allocate a buffer of the specified batch size in anticipation of additional records." )
                .withOptionalArg().ofType( Integer.class ).defaultsTo( 2000 );
        OptionSpec<String> clientId = parser
                .accepts(
                        "client.id",
                        "The client id is a user-specified string sent in each request to help trace calls. It should logically identify the application making the request." )
                .withOptionalArg().ofType( String.class ).defaultsTo( "" );
        OptionSpec<Integer> sendBufferBytes = parser.accepts( "send.buffer.bytes", "Socket write buffer size" )
                .withOptionalArg().ofType( Integer.class ).defaultsTo( 100 * 1024 );
    }

    public static OptionParser getStandardOptionParser() {

        OptionParser parser = new OptionParser();
        addStandardOptionsToParser( parser );

        return parser;
    }
    
}
