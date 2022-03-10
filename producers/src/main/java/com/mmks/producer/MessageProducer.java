package com.mmks.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class MessageProducer {

    private static final Logger logger = LoggerFactory.getLogger(MessageProducer.class);

    String topicName = "test-topic-replicated";
    KafkaProducer<String, String> kafkaProducer;

    public MessageProducer(Map<String, Object> propsMap) {
        kafkaProducer = new KafkaProducer<String, String>(propsMap);
    }

    public static Map<String, Object> propsMap() {
        Map<String, Object> propsMap = new HashMap<>();
        propsMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092, localhost:9093, localhost:9094");
        propsMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propsMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propsMap.put(ProducerConfig.ACKS_CONFIG, "all");
        propsMap.put(ProducerConfig.RETRIES_CONFIG, 10);
        propsMap.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 3000);
        return propsMap;
    }

    public void publishMessageSync(String key, String value) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topicName, key, value);
        try {
            RecordMetadata recordMetadata = kafkaProducer.send(producerRecord).get();
            logger.info("Partition: {} , offset: {}", recordMetadata.partition(), recordMetadata.offset());
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Exception in publishMessageSync {}", e.getMessage());
            e.printStackTrace();
        }
    }


    public void publishMessageAsync(String key, String value) throws ExecutionException, InterruptedException {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, key, value);
        kafkaProducer.send(producerRecord, (RecordMetadata metadata, Exception exception) -> {
            if (exception != null) {
                logger.error("Exception in callback {}", exception.getMessage());
            } else {
                logger.info("Partition: {} , offset: {}", metadata.partition(), metadata.offset());
            }
        }).get();
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        MessageProducer messageProducer = new MessageProducer(propsMap());
//        messageProducer.publishMessageSync(null, "value with no key 123");
        messageProducer.publishMessageAsync(null, "in sync replicas backoff ****** ");
//        Thread.sleep(3000);
    }
}
