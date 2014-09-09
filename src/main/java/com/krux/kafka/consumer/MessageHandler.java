package com.krux.kafka.consumer;

public interface MessageHandler<T> {
    
    public void onMessage(T message);

}
