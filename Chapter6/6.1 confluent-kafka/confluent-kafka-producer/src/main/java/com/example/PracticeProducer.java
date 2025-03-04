package com.example;

import com.example.config.CustomPartition;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PracticeProducer {
    private final static Logger logger = LoggerFactory.getLogger(PracticeProducer.class);
    private final static String TOPIC = "test";
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartition.class);
//        config.put(ProducerConfig.ACKS_CONFIG, "0");

        KafkaProducer<String, String> producer = new KafkaProducer<>(config);

//        //key => null
//        String testMessage = "This is a test message2";
//        ProducerRecord<String,String> record = new ProducerRecord<>(TOPIC, testMessage);
//        RecordMetadata metadata1 = producer.send(record).get();
//        logger.info(metadata1.toString());
        //key is used
        ProducerRecord<String,String> record2 = new ProducerRecord<>(TOPIC, "test-key1", "test-value1");

        //topic 이름 - 파티션 번호 @ 오프셋 번호 식으로 출력된다. (오프셋 -1은 acks 값을 0으로 설정했을 때 응답값을 받지 못해 출력하는 값)
        RecordMetadata metadata2 = producer.send(record2).get();
        logger.info(metadata2.toString());

        //partition
        ProducerRecord<String,String> record3 = new ProducerRecord<>(TOPIC, "test-custom-partition", "33");
        RecordMetadata metadata3 = producer.send(record3).get();
        logger.info(metadata3.toString());


        producer.flush();
        //항상 리소스 해제 지킬 것
        producer.close();
    }
}
