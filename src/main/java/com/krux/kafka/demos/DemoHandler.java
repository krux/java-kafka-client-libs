package com.krux.kafka.demos;

import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.krux.kafka.consumer.MessageHandler;

public class DemoHandler<T> implements MessageHandler {
    
    private static final Logger LOG = LoggerFactory.getLogger( DemoHandler.class );
    
    private final AtomicLong messageCount = new AtomicLong(0);
    @Override
    
    public void onMessage( Object message ) {
        //LOG.info( Thread.currentThread().getName() + ": " + (new String((byte[])message)) );
        messageCount.incrementAndGet();
    }
    
    public long getCount() {
        return messageCount.get();
    }

}
