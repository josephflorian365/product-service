package com.nttdata.productservice.config;

import com.nttdata.productservice.messaging.ClientValidationRequest;
import com.nttdata.productservice.messaging.ClientValidationResponse;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaRequestReplyConfig {

    @Value("${spring.kafka.bootstrap-servers:${KAFKA_BOOTSTRAP_SERVERS:kafka:9093}}")
    private String bootstrapServers;

    @Value("${topics.client-validation-request}")
    private String requestTopic;

    @Value("${topics.client-validation-reply}")
    private String replyTopic;

    @Bean
    ProducerFactory<String, ClientValidationRequest> clientValidationProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    ConsumerFactory<String, ClientValidationResponse> clientValidationConsumerFactory() {
        JsonDeserializer<ClientValidationResponse> deserializer =
            new JsonDeserializer<>(ClientValidationResponse.class);
        deserializer.addTrustedPackages("*");

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "product-service-replies");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), deserializer);
    }

    @Bean
    ConcurrentMessageListenerContainer<String, ClientValidationResponse> repliesContainer() {
        org.springframework.kafka.listener.ContainerProperties containerProperties =
            new org.springframework.kafka.listener.ContainerProperties(replyTopic);
        ConcurrentMessageListenerContainer<String, ClientValidationResponse> container =
            new ConcurrentMessageListenerContainer<>(clientValidationConsumerFactory(), containerProperties);
        container.getContainerProperties().setGroupId("product-service-replies");
        return container;
    }

    @Bean
    ReplyingKafkaTemplate<String, ClientValidationRequest, ClientValidationResponse> replyingKafkaTemplate() {
        return new ReplyingKafkaTemplate<>(clientValidationProducerFactory(), repliesContainer());
    }

    @Bean
    NewTopic clientValidationRequestTopic() {
        return new NewTopic(requestTopic, 1, (short) 1);
    }

    @Bean
    NewTopic clientValidationReplyTopic() {
        return new NewTopic(replyTopic, 1, (short) 1);
    }
}
