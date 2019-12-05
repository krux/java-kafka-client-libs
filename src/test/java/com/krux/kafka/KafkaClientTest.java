package com.krux.kafka;

import com.krux.kafka.consumer.KafkaConsumer;
import com.krux.kafka.producer.KafkaProducer;
import com.salesforce.kafka.test.junit4.SharedKafkaTestResource;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.*;

public class KafkaClientTest {

    private static final String TOPIC = "test";

    /**
     * We have a single embedded Kafka server that gets started when this test class is initialized.
     *
     * It's automatically started before any methods are run via the @RegisterExtension annotation.
     * It's automatically stopped after all of the tests are completed via the @RegisterExtension annotation.
     */
    @ClassRule
    public static final SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource();

    @Before
    public void setup() {
        System.out.println("Using connect string " + sharedKafkaTestResource.getKafkaConnectString());
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, sharedKafkaTestResource.getKafkaConnectString());
        AdminClient adminClient = AdminClient.create(props);
        NewTopic newTopic = new NewTopic(TOPIC, 3, (short)1);
        List<NewTopic> newTopics = new ArrayList<NewTopic>();
        newTopics.add(newTopic);
        CreateTopicsResult res = adminClient.createTopics(newTopics);
        System.out.println("Topics created " + res.values().keySet().toString());
        adminClient.close();
    }

    @Test
    public void send() {
        Properties props = new Properties();
        props.put("producer.bootstrap.servers", sharedKafkaTestResource.getKafkaConnectString());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        KafkaProducer producer = new KafkaProducer(props, TOPIC);
        try {
            RecordMetadata message1 = producer.send("Testing message1").get();
            RecordMetadata message2 = producer.send("Testing message2").get();
            RecordMetadata message3 = producer.send("Testing message3").get();
            System.out.println("Message 1 was send to partition " + message1.partition());
            System.out.println("Message 2 was send to partition " + message2.partition());
            System.out.println("Message 3 was send to partition " + message3.partition());

            Set<String> hash_Set = new HashSet<String>();
            hash_Set.add(Integer.toString(message1.partition()));
            hash_Set.add(Integer.toString(message2.partition()));
            hash_Set.add(Integer.toString(message3.partition()));
            assert(hash_Set.size() > 1); // Make sure the messages were at least sent to 2 unique partitions
        } catch (Exception e) {
            assert(false);
        }
    }

    @Test
    public void read() {
        OptionParser parser = new OptionParser();
        KafkaConsumer.addStandardOptionsToParser(parser);
        OptionSet options = parser.parse( "topic.threads=" + TOPIC + ",1",
                "producer.bootstrap.servers=" + sharedKafkaTestResource.getKafkaConnectString(),
                "group.id=test1");
        KafkaConsumer consumer = new KafkaConsumer(options, new TestHandler());
        consumer.start();
    }
}
