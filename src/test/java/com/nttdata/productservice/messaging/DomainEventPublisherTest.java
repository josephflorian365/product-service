package com.nttdata.productservice.messaging;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

@ExtendWith(MockitoExtension.class)
class DomainEventPublisherTest {

    private DomainEventPublisher domainEventPublisher;

    @BeforeEach
    void setUp() {
        domainEventPublisher = new DomainEventPublisher();
        ReflectionTestUtils.setField(domainEventPublisher, "productEventsTopic", "product-events");
    }

    @Test
    void shouldSkipDomainEventPublicationWhenPublisherIsDisabled() {
        assertDoesNotThrow(() -> domainEventPublisher.publish(
            "YANKI_WALLET_CREATED",
            "YANKI_WALLET",
            "wallet-1",
            Map.of("phoneNumber", "999888777")
        ));
    }
}
