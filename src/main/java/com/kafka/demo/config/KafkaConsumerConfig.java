package com.kafka.demo.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka消费端配置
 */
@Slf4j
@Configuration
@EnableKafka
public class KafkaConsumerConfig {

    @Autowired
    private PropertiesConfig propertiesConfig;

    /**
     * 配置监听，将消费工厂信息配置进去
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        // 多线程消费
        factory.setConcurrency(propertiesConfig.getKafkaConsumerConcurrency());

        // 设置批量消费模式
        factory.setBatchListener(propertiesConfig.getKafkaConsumerBatchListener());

        // 设置手动提交模式（如果enable-auto-commit为false）
        if (!propertiesConfig.getKafkaConsumerAutoCommit()) {
            factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        }

        // 设置poll超时时间
        factory.getContainerProperties().setPollTimeout(3000);

        log.info("Kafka监听器容器工厂初始化完成: concurrency={}, ackMode={}, batchListener={}",
                propertiesConfig.getKafkaConsumerConcurrency(),
                propertiesConfig.getKafkaConsumerAutoCommit(),
                propertiesConfig.getKafkaConsumerBatchListener());

        return factory;
    }

    /**
     * 消费消费工厂
     */
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    /**
     * 消费配置
     */
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>(20);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, propertiesConfig.getKafkaBootstrapServer());
        // Kafka 消息的序列化方式
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        props.put(ConsumerConfig.GROUP_ID_CONFIG, propertiesConfig.getKafkaConsumerGroupId());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, propertiesConfig.getKafkaConsumerAutoCommit());
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, propertiesConfig.getKafkaConsumerAutoCommitInterval());
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, propertiesConfig.getKafkaConsumerSessionTimeout());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, propertiesConfig.getKafkaConsumerMaxPollRecords());
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, propertiesConfig.getKafkaConsumerMaxPartitionFetchBytes());
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, propertiesConfig.getKafkaConsumerFetchMaxBytes());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3000);

        log.info("CDC Kafka消费者工厂初始化完成: servers={}, group={}, maxPollRecords={}",
                propertiesConfig.getKafkaBootstrapServer(), propertiesConfig.getKafkaConsumerGroupId(),
                propertiesConfig.getKafkaConsumerMaxPollRecords());
        return props;
    }
}