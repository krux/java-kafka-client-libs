Krux Kafka CLient Library
=========================

The Krux Kafka Client library as a simple wrapper for the [Kafka](http://kafka.apache.org)-provided client libraries, both producers and consumers, that's built atop the [Krux Standard Library](https://github.com/krux/java-stdlib) and automatically emits tons of useful usage statistics (such as messages processed rates and processing time histograms) on a per-topic basis.  It also provides a pattern for configuring multiple topics to be handled by a single, thread-safe consumer, a common pattern at Krux.

Simple Use
----------
```
/* Krux' Kafka consumer client handles threads, stats, status for you */

/* first, implement the MessageHandler interface */
public interface MessageHandler<T extends Object> {
    public void onMessage( T message );
}
```
```
/* Then instantiate, pass to Krux' Kafka Consumer, .start() */
final MessageHandler<Object> myHandler = new MySimpleHandler<Object>();
KafkaConsumer consumer = new KafkaConsumer( options, myHandler );

//handles message consumption in a non-daemon thread
consumer.start();
```

More Advanced Use
-----------------
See example [consumer](https://github.com/krux/java-kafka-client-libs/blob/master/src/main/java/com/krux/kafka/demos/DemoConsumer.java) and [producer](https://github.com/krux/java-kafka-client-libs/blob/master/src/main/java/com/krux/kafka/demos/DemoProducer.java) applications.


