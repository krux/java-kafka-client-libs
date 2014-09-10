package com.krux.kafka.producer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaProducer {
    
    private static final Logger LOG = LoggerFactory.getLogger( KafkaProducer.class );

    private final Producer<byte[], byte[]> _producer;
    private final String _topic;
    
    private static final List<Producer> _producers = Collections.synchronizedList( 
            new ArrayList<Producer>() );
    
    static {
        Runtime.getRuntime().addShutdownHook( new Thread() {
            @Override
            public void run() {
                LOG.info( "Shutting down producers" );
                for ( Producer producer : _producers ) {
                    producer.close();
                }
            }
        } );
    }

    public KafkaProducer( Properties props, String topic ) {

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
    
    public void send( byte[] key, byte[] message ) {
        KeyedMessage<byte[], byte[]> data = new KeyedMessage<byte[], byte[]>( _topic, key, message );
        _producer.send( data );
    }
}