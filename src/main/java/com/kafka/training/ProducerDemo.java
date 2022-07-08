package com.kafka.training;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static Logger logger = LoggerFactory.getLogger(ProducerDemo.class);

    static ObjectMapper objectMapper = new ObjectMapper();

    static ObjectWriter writer = objectMapper.writer();

    public static void main(String[] args) throws InterruptedException, JsonProcessingException {
        String bootstrapServers = "127.0.0.1:9092";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for(int i=0; i<5; i++) {
            Thread.sleep(100);
            Employee e = new Employee();
            e.setAge(i);
            if(i%2==0)
                e.setName("Lee_"+i);
            else
                e.setName("Tee_"+i);
            ProducerRecord<String, String> record = new ProducerRecord<>("streams-plaintext-input", Integer.toString(i),writer.writeValueAsString(e));
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e==null){
                        logger.info("Message Sent: \n Topic: {}, \n Partition: {} \n Offset: {} \n Timestamp: {}", recordMetadata.topic(), recordMetadata.partition(),recordMetadata.offset(),recordMetadata.timestamp());
                    }else{
                        logger.error("Error sending messages: {}",e.getMessage());
                    }
                }
            });
        }
        producer.flush();

        producer.close();
    }
}

class Employee{
    String name;
    Integer age;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }
}