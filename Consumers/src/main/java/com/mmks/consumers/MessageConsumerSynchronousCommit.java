package com.mmks.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MessageConsumerSynchronousCommit {

    private static final Logger logger = LoggerFactory.getLogger(MessageConsumerSynchronousCommit.class);

    KafkaConsumer<String, String> kafkaConsumer;
    String topicName = "test-topic-replicated";

    public MessageConsumerSynchronousCommit(Map<String, Object> propsMap) {
        kafkaConsumer = new KafkaConsumer<String, String>(propsMap);
    }

    public static Map<String, Object> buildConsumerProperties() {
        Map<String, Object> propsMap = new HashMap<>();
        propsMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        propsMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        propsMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        propsMap.put(ConsumerConfig.GROUP_ID_CONFIG, "messageconsumer2");
        propsMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        propsMap.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
//        propsMap.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 20000);

        return propsMap;
    }

    public static void main(String[] args) {
        MessageConsumerSynchronousCommit messageConsumer = new MessageConsumerSynchronousCommit(buildConsumerProperties());
        messageConsumer.pollKafka();
    }

    public void pollKafka() {
        kafkaConsumer.subscribe(List.of(topicName), new ConsumerRebalanceListener() {

            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println("Partition Revoked");
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.println("Partition Assigned");
            }
        });
        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
                consumerRecords.forEach(stringStringConsumerRecord -> {
                    System.out.println(stringStringConsumerRecord.value());
                });

                if (consumerRecords.count() > 0) {
                    kafkaConsumer.commitSync();
                }
            }
        } catch (Exception e) {
            logger.error("Error during poll {}", e.getMessage());
        } finally {
            kafkaConsumer.close();
        }
    }
}
