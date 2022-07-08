package com.kafka.training;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.function.Consumer;

public class ConsumerDemo {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

    private static String bootstrapServer = "127.0.0.1:9092";
    private static String[] topics = {"second_topic"};

    public static void main(String[] arg){
        Properties config = new Properties();
        config.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        config.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        config.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"cg3");

        KafkaConsumer<String, String> consumer = new KafkaConsumer(config);
        consumer.subscribe(Arrays.asList(topics));

        while(true){
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(1000));

            for(ConsumerRecord<String, String> record: records){
                logger.info("{} -> {} :: {}",record.topic(),record.key(),record.value());
            }
        }

    }
}
