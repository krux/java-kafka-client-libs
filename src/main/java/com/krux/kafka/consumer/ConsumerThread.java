package com.krux.kafka.consumer;

import com.krux.stdlib.KruxStdLib;
import com.krux.stdlib.statsd.StatsdClient;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumerThread implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger( ConsumerThread.class );
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final MessageHandler<byte[]> _handler;
    private final org.apache.kafka.clients.consumer.KafkaConsumer<byte[], byte[]> _consumer;
    private String _topic;
    private long _consumerPollTimeout;
    private StatsdClient _statsd;

    public ConsumerThread(
            org.apache.kafka.clients.consumer.KafkaConsumer<byte[], byte[]> consumer,
            long consumerPollTimeout, 
            String topic, 
            MessageHandler handler
        ) {
        this(consumer, consumerPollTimeout, topic, handler, KruxStdLib.get().getStatsdClient());
    }

    public ConsumerThread(
            org.apache.kafka.clients.consumer.KafkaConsumer<byte[], byte[]> consumer,
            long consumerPollTimeout, 
            String topic, 
            MessageHandler handler,
            StatsdClient statsd
        ) {
        _consumer = consumer;
        _consumerPollTimeout = consumerPollTimeout;
        _topic = topic;
        _handler = handler;
        _statsd = statsd;
    }

    @Override
    public void run() {
        try {
            // Subscribe to the topic
            _consumer.subscribe( Arrays.asList(_topic) );
            LOG.info( "Consuming thread subscribed - " + _topic);
            while (!closed.get()) {
                // Poll for messages in queue
                ConsumerRecords<byte[], byte[]> records = _consumer.poll(_consumerPollTimeout);
                for (ConsumerRecord<byte[], byte[]> record : records) {

                    long start = System.currentTimeMillis();
                    byte[] message = record.value();
                    LOG.debug("message received: {}", (new String(message)));

                    _handler.onMessage(message);

                    long time = System.currentTimeMillis() - start;
                    _statsd.time("message_received." + _topic, time);
                }
            }
        } catch (WakeupException e) {
            // Ignore exception if closing
            if (!closed.get()) throw e;
        } catch (Exception e) {
            LOG.error("Consumer failure:", e);
        } finally {
            LOG.warn("Consumer shutting down");
            _consumer.close();
        }
    }

    // Shutdown hook which can be called from a separate thread
    public void shutdown() {
        closed.set(true);
        _consumer.wakeup();
    }
}
