package com.krux.kafka.consumer;

import java.util.concurrent.atomic.AtomicLong;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.krux.stdlib.KruxStdLib;

public class ConsumerThread implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger( ConsumerThread.class );
    //private static AtomicLong totalMessages = new AtomicLong( 0 );

    private final KafkaStream<byte[], byte[]> _stream;
    private final MessageHandler<byte[]> _handler;
    private String _topic;

    public ConsumerThread( KafkaStream<byte[], byte[]> stream, String topic, MessageHandler handler ) {
        _stream = stream;
        _handler = handler;
        _topic = topic;
    }

    @Override
    public void run() {
        LOG.info( "Attempting to receive messages" );

        try {
            ConsumerIterator<byte[], byte[]> it = _stream.iterator();
            while ( it.hasNext() ) {
                long start = System.currentTimeMillis();
                //totalMessages.incrementAndGet();

                byte[] message = it.next().message();

                if ( LOG.isDebugEnabled() ) {
                    LOG.debug( "message received: " + ( new String( message ) ) );
                }

                _handler.onMessage( message );

                long time = System.currentTimeMillis() - start;
                KruxStdLib.STATSD.time( "message_received." + _topic, time );
                //KruxStdLib.STATSD.time( "message_received_all", time );

            }
        } catch ( Exception e ) {
            if ( e instanceof InterruptedException ) {
                LOG.warn( "Consumer group threads interrupted: " + e.getLocalizedMessage() );
            } else {
                LOG.error( "no longer fetching messages", e );
            }
        }
    }
}