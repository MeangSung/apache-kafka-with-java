package com.example;

import com.example.config.RebalanceListener;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PracticeConsumer {
    private final static Logger logger = LoggerFactory.getLogger(PracticeConsumer.class);
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private final static String GROUP_ID = "test-group";
    private final static String TOPIC = "test";
    private final static int PARTITION_NUM = 1;
    private static KafkaConsumer<String, String> consumer;

    public static void main(String[] args) {

        Properties config = new Properties();

        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);
        config.put(ConsumerConfig.GROUP_ID_CONFIG,GROUP_ID);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 자동 커밋 수행
//        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
//
//            consumer.subscribe(Collections.singletonList(TOPIC));
//
//            while (true) {
//                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
//                for (ConsumerRecord<String, String> record : records) {
//                    logger.info("record : {}", record);
//                }
//            }
//        } catch (Exception e){
//            throw new RuntimeException(e);
//        }

        //수동 커밋 수행 - 동기
//        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
//
//        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config);
//        consumer.subscribe(Collections.singletonList(TOPIC));
//
//        while (true) {
//            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1));
//            for (ConsumerRecord<String, String> record : records) {
//                logger.info("record : {}", record);
//            }
//            consumer.commitSync();
//        }

        //수동 커밋 - 동기 (레코드 단위)
//        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
//
//        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(config);
//        consumer.subscribe(Collections.singletonList(TOPIC));
//
//        while(true){
//            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1));
//            Map<TopicPartition, OffsetAndMetadata> currentOffset = new HashMap<>();
//            for(ConsumerRecord<String, String> record : records){
//                logger.info("record : {}", record);
//                currentOffset.put(
//                        new TopicPartition(record.topic(), record.partition()),
//                        new OffsetAndMetadata(record.offset()+1, null)
//                );
//                consumer.commitSync(currentOffset);
//            }
//        }

        //수동 커밋 - 비동기 처리 (커밋이 완료됐는지 확인)
//        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
//
//        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config);
//        consumer.subscribe(Collections.singletonList(TOPIC));
//        while (true) {
//            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1));
//            for (ConsumerRecord<String, String> record : records) {
//                logger.info("record : {}", record);
//            }
//            consumer.commitAsync(new OffsetCommitCallback() {
//                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception e) {
//                    if (e != null) {
//                        System.err.println("Error committing offsets");
//                    }
//                    else{
//                        System.out.println("Committed offsets");
//                    }
//                    if (e != null) {
//                        logger.error("Error committing offsets {}",offsets, e);
//                    }
//                }
//            });
//        }

        //RebalanceListener 적용

//        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config);
//        consumer.subscribe(Collections.singletonList(TOPIC), new RebalanceListener());
//        while(true){
//            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
//            for(ConsumerRecord<String, String> record : records){
//                logger.info("record : {}", record);
//            }
//        }

        // 특정 토픽의 partition 을 등록하는 경우
//        consumer = new KafkaConsumer<>(config);
//
//        consumer.assign(Collections.singletonList(new TopicPartition(TOPIC, PARTITION_NUM)));
//
//            while (true) {
//                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1));
//                for (ConsumerRecord<String, String> record : records) {
//                    logger.info("record: {}", record);
//                }
//
//            }

        //안전한 종료
        Runtime.getRuntime().addShutdownHook(new ShutDownThread());

        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumer = new KafkaConsumer<>(config);
        consumer.subscribe(Collections.singletonList(TOPIC));

        try{
            while(true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1));
                for(ConsumerRecord<String, String> record : records){
                    logger.info("record : {}", record);
                }
                consumer.commitSync();
            }
        }
        catch (WakeupException e){
            logger.warn("Wake up Exception");
        }
        finally {
            logger.warn("Consumer closed");
            consumer.close();
        }

    }

    static class ShutDownThread extends Thread{
        public void run(){
            logger.warn("Shutdown hook");
            consumer.wakeup();
        }
    }
}
