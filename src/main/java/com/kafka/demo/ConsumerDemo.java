package com.kafka.demo;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ConsumerDemo {

    /**
     * 自动提交模式消费者
     * 使用配置文件中设置的自动提交（enable-auto-commit=true）
     */
    @KafkaListener(topics = "${spring.kafka.producer.topic}",
            groupId = "${spring.kafka.consumer.group-id}")
    public void listenerMessage(ConsumerRecord<String, String> record) {
        try {
            log.info("收到消息 [主题: {}, 分区: {}, 偏移量: {}, 时间戳: {}], 内容: {}",
                    record.topic(), record.partition(), record.offset(),
                    record.timestamp(), record.value());
        } catch (Exception e) {
            log.error("处理消息时发生错误: 主题={}, 分区={}, 偏移量={}",
                    record.topic(), record.partition(), record.offset(), e);
            // 根据业务需求决定是否抛出异常
            // 如果抛出异常，会触发重试机制（如果配置了重试）
            throw new RuntimeException("消息处理失败", e);
        }
    }

    /**
     * 手动提交模式消费者（当enable-auto-commit=false时使用）
     * 注意：需要取消配置文件中的自动提交
     */
    // @KafkaListener(topics = "${spring.kafka.producer.topic}",
    //                groupId = "${spring.kafka.consumer.group-id}")
    public void listenerMessageWithAck(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            log.info("收到消息 [主题: {}, 分区: {}, 偏移量: {}, 时间戳: {}], 内容: {}",
                    record.topic(), record.partition(), record.offset(),
                    record.timestamp(), record.value());

            // 手动提交偏移量
            ack.acknowledge();
            log.info("手动提交偏移量成功: 分区={}, 偏移量={}", record.partition(), record.offset());

        } catch (Exception e) {
            log.error("处理消息失败，不提交偏移量: 主题={}, 分区={}, 偏移量={}",
                    record.topic(), record.partition(), record.offset(), e);
            // 不调用ack.acknowledge()，消息将重新消费
            // 根据业务需求决定是否抛出异常
            // throw new RuntimeException("消息处理失败", e);
        }
    }
}