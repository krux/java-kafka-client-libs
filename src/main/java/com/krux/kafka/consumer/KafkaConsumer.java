package com.krux.kafka.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumer {

    private static final Logger LOG = LoggerFactory.getLogger( KafkaConsumer.class );

    private final Map<String, Integer> _topicMap;
    private final Map<String, ConsumerConnector> _topicConsumers;
    private final MessageHandler<?> _handler;
    final Map<String, ExecutorService> _executors;
    
    public KafkaConsumer( Properties props, Map<String, Integer> topicMap, MessageHandler<?> handler ) {
        _executors = new HashMap<String, ExecutorService>();
        _topicConsumers = new HashMap<String, ConsumerConnector>();

        for ( String topic : topicMap.keySet() ) {
            ConsumerConfig topicConfig = new ConsumerConfig( props );
            _topicConsumers.put( topic, kafka.consumer.Consumer.createJavaConsumerConnector( topicConfig ) );
        }
        _topicMap = topicMap;
        _handler = handler;
    }

    public void start() {

        LOG.info( "***********Creating consumers: " );
        for ( String topic : _topicMap.keySet() ) {
            LOG.info( "***********Creating consumer for topic : " + topic );
            Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = _topicConsumers.get( topic ).createMessageStreams(
                    _topicMap );
            
            for ( String key : consumerMap.keySet() ) {
                LOG.info( "streams key : " + key  );
            }

            List<KafkaStream<byte[], byte[]>> streams = consumerMap.get( topic );
            LOG.info( "streams.size() : " + streams.size()  );

            // now create an object to consume the messages
            ExecutorService executor = Executors.newFixedThreadPool( _topicMap.get( topic ) );
            _executors.put( topic, executor );
            LOG.info( "***********Creating executor for topic : " + topic );
            for ( final KafkaStream<byte[], byte[]> stream : streams ) {
                LOG.info( "***********Creating stream thread for stream " );
                executor.submit( new ConsumerThread( stream, topic, _handler ) );
                LOG.info( "***********Created stream thread for stream " );
            }
        }

        Runtime.getRuntime().addShutdownHook( new Thread() {
            @Override
            public void run() {
                LOG.info( "Shutting down consumer thread pools" );
                for ( String key : _topicMap.keySet() ) {
                    ExecutorService executor = _executors.get( key );
                    executor.shutdownNow();
                }
            }
        } );
    }

    public void stop() {
        LOG.info( "Shutting down consumer thread pools" );
        for ( String key : _topicMap.keySet() ) {
            ExecutorService executor = _executors.get( key );
            executor.shutdownNow();
        }
    }
}
