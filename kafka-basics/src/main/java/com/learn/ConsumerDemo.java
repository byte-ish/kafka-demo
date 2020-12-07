package com.learn;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());
        String boostrapServers = "localhost:9092";
        String groupId = "my-fourth-application";
        String topic = "first_topic";


        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create a consumer
        KafkaConsumer<String, String> stringStringKafkaConsumer = new KafkaConsumer<>(properties);

        //subscribe consumer to our topic(s)
        stringStringKafkaConsumer.subscribe(Collections.singleton(topic));

        //poll for new data
        while (true) {
            ConsumerRecords<String, String> stringStringConsumerRecords
                    = stringStringKafkaConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord record : stringStringConsumerRecords) {
                logger.info("Key: {} and value: {}", record.key(), record.value());

            }
        }


    }
}
