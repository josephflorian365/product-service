package com.nttdata.productservice.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;

@Component
public class DomainEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(DomainEventPublisher.class);

    @Value("${topics.product-events:product.events}")
    private String productEventsTopic;

    public void publish(String eventType, String aggregateType, String aggregateId, Map<String, Object> payload) {
        DomainEvent event = new DomainEvent(
            UUID.randomUUID().toString(),
            eventType,
            aggregateType,
            aggregateId,
            LocalDateTime.now(),
            payload
        );

        log.warn(
            "Domain event publication is disabled; skipping eventType={} aggregateId={} topic={}",
            eventType,
            aggregateId,
            productEventsTopic
        );
    }
}
