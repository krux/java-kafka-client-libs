package com.krux.kafka.consumer;

public interface MessageHandler<T extends Object> {

    public void onMessage( T message );
}
