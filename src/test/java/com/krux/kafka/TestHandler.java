package com.krux.kafka;

import com.krux.kafka.consumer.MessageHandler;

class TestHandler<T> implements MessageHandler {
    @Override
    public void onMessage( Object message ) {
        System.out.println( new String( (byte[]) message ) );
    }
}
