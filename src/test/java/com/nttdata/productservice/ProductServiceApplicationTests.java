package com.nttdata.productservice;

import com.nttdata.productservice.messaging.ClientValidationRequest;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;

@SpringBootTest(properties = {
    "eureka.client.enabled=false",
    "spring.cloud.discovery.enabled=false",
    "spring.kafka.admin.auto-create=false",
    "spring.kafka.admin.fail-fast=false",
    "spring.kafka.bootstrap-servers=localhost:9092",
    "kafka.bootstrap-servers=localhost:9092",
    "spring.data.mongodb.uri=mongodb://localhost:27017/testdb",
    "spring.data.redis.host=localhost",
    "spring.data.redis.port=6379"
})
class ProductServiceApplicationTests {

    @MockBean(name = "repliesContainer")
    private ConcurrentMessageListenerContainer<String, String> repliesContainer;

    @MockBean
    private ReplyingKafkaTemplate<String, ClientValidationRequest, String> replyingKafkaTemplate;

    @Test
    void contextLoads() {
    }
}
