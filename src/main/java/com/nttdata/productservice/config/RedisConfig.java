package com.nttdata.productservice.config;

import com.nttdata.productservice.model.ClientSummary;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@Configuration
public class RedisConfig {

    @Bean
    ReactiveRedisTemplate<String, ClientSummary> clientSummaryRedisTemplate(
            ReactiveRedisConnectionFactory factory) {
        Jackson2JsonRedisSerializer<ClientSummary> serializer =
            new Jackson2JsonRedisSerializer<>(ClientSummary.class);
        RedisSerializationContext<String, ClientSummary> context = RedisSerializationContext
            .<String, ClientSummary>newSerializationContext(new StringRedisSerializer())
            .value(serializer)
            .build();
        return new ReactiveRedisTemplate<>(factory, context);
    }
}
