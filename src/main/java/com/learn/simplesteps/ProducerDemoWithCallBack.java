package com.learn.simplesteps;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.stream.IntStream;


public class ProducerDemoWithCallBack {
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallBack.class);
        //create producer properties

        Properties properties = new Properties();

        String boostrapServers = "localhost:9092";

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create Kafka Producer

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        IntStream.range(0, 20).forEach(i -> {
            //create a producer record
            ProducerRecord<String, String> stringStringProducerRecord = new ProducerRecord<>("first_topic", "hello world".concat(String.valueOf(i)));
            //Send data
            producer.send(stringStringProducerRecord, (recordMetadata, e) -> {
                //executes every time a record is sent successfully
                if (e == null) {
                    //record sent successfully
                    logger.info("received new meta data.\n Topic:{}, Partition:{}, Offset:{}, Time:{}  "
                            , recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
                } else {
                    logger.error("Error while producing:", e);

                }
            });
        });

        producer.flush();
        producer.close();

    }
}
