package com.kafka.demo;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
public class ProducerDemo {


    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Value("${spring.kafka.producer.topic}")
    private String topic;

    @RequestMapping("/send")
    public String sendMessage() {
        try {
//            //消息的内容
//            String value = "this is the message's value";
//            ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, value);
//            SendResult<String, String> result = future.get();

            for (int i = 0; i < 10; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, "test-key" + i, "{\"before\":null,\"after\":{\"id\":85592,\"mongodb_id\":\"69b023bea8fcd17d8ceaa588\",\"user_id\":\"61710e55012637185f1b1ba4\",\"name\":\"学员_11579370\",\"gender\":\"\",\"mobile\":\"12378903651\",\"source\":\"inte\",\"source_channel\":\"数据智能\",\"source_channel_detail\":\"试听课\",\"keyword\":\"\",\"search_word\":\"\",\"degree\":\"\",\"career\":\"\",\"status\":\"unprocessed\",\"weixin\":\"\",\"qq\":\"\",\"email\":\"\",\"id_type\":\"\",\"id_number\":\"\",\"graduated_at\":null,\"zip_code\":\"\",\"total_paid_fee\":\"AA==\",\"total_received_fee\":\"AA==\",\"deleted_flag\":false,\"consult_class\":\"\",\"consult_content\":\"\",\"consult_course_type\":\"\",\"creator_no\":0,\"creator_name\":\"智能数据\",\"creator_department_no\":195,\"creator_department_name\":\"网站运营\",\"expect_class_area_province\":\"\",\"expect_class_area_city\":\"\",\"follower_no\":0,\"follower_name\":\"\",\"follower_department_no\":0,\"follower_department_name\":\"\",\"assigner_no\":0,\"assigner_name\":\"\",\"assigner_department_no\":0,\"assigner_department_name\":\"\",\"usual_address_province\":\"\",\"usual_address_city\":\"\",\"usual_address_country\":\"\",\"express_address_province\":\"\",\"express_address_city\":\"\",\"");
                ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(record);
            }

            return "send message success: ";
        } catch (Exception e) {
            log.error("send message failed.", e);
        }
        return "send message failed.";
    }
}
