package com.example.config;

import java.util.Collection;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RebalanceListener implements ConsumerRebalanceListener {
    private final static Logger logger = LoggerFactory.getLogger(RebalanceListener.class);

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        logger.warn("Partitions revoked: {}", partitions.toString());
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
        logger.warn("Partitions assigned: {}", collection.toString());
    }
}
