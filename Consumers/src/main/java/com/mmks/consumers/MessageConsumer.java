package com.mmks.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MessageConsumer {

    private static final Logger logger = LoggerFactory.getLogger(MessageConsumer.class);

    KafkaConsumer<String, String> kafkaConsumer;
    String topicName = "test-topic-replicated";

    public MessageConsumer(Map<String, Object> propsMap) {
        kafkaConsumer = new KafkaConsumer<String, String>(propsMap);
    }

    public static Map<String, Object> buildConsumerProperties() {
        Map<String, Object> propsMap = new HashMap<>();
        propsMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        propsMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        propsMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        propsMap.put(ConsumerConfig.GROUP_ID_CONFIG, "messageconsumer2");
        propsMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        propsMap.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 20000);

        return propsMap;
    }

    public static void main(String[] args) {
        MessageConsumer messageConsumer = new MessageConsumer(buildConsumerProperties());
        messageConsumer.pollKafka();
    }

    public void pollKafka() {
        kafkaConsumer.subscribe(List.of(topicName));
        try {
            while (true) {
                ConsumerRecords<String, String> poll = kafkaConsumer.poll(Duration.ofMillis(100));
                poll.forEach(stringStringConsumerRecord -> {
                    System.out.println(stringStringConsumerRecord.value());
                });
            }
        } catch (Exception e) {
            logger.error("Error during poll {}", e.getMessage());
        } finally {
            kafkaConsumer.close();
        }
    }
}
