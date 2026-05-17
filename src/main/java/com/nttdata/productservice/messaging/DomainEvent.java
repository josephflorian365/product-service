package com.nttdata.productservice.messaging;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DomainEvent {

    private String eventId;

    private String eventType;

    private String aggregateType;

    private String aggregateId;

    private LocalDateTime occurredAt;

    private Map<String, Object> payload;
}
