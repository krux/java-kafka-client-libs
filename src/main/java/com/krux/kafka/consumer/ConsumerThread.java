package com.krux.kafka.consumer;

import java.util.concurrent.atomic.AtomicLong;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.krux.stdlib.KruxStdLib;

public class ConsumerThread implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerThread.class);
    private static AtomicLong totalMessages = new AtomicLong(0);

    private final KafkaStream<byte[], byte[]> _stream;
    private final MessageHandler<Object> _handler;

    public ConsumerThread(KafkaStream<byte[], byte[]> stream, String topic, MessageHandler<Object> handler) {
        _stream = stream;
        _handler = handler;
    }

    @Override
    public void run() {
        LOG.info("Attempting to receive messages");

        try {
            ConsumerIterator<byte[], byte[]> it = _stream.iterator();
            while (it.hasNext()) {
                long start = System.currentTimeMillis();
                totalMessages.incrementAndGet();

                byte[] message = it.next().message();

                if (LOG.isDebugEnabled()) {
                    LOG.debug("message received: " + message);
                }

                _handler.onMessage( message );

                long time = System.currentTimeMillis() - start;
                KruxStdLib.STATSD.time("kafka_message_received", time);

            }
        } catch (Exception e) {
            LOG.error("no longer fetching messages", e);
        }
    }
}