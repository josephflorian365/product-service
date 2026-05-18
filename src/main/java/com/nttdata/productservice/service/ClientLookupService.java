package com.nttdata.productservice.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nttdata.productservice.exception.BusinessException;
import com.nttdata.productservice.model.ClientSummary;
import com.nttdata.productservice.messaging.ClientValidationRequest;
import com.nttdata.productservice.messaging.ClientValidationResponse;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator;
import io.github.resilience4j.reactor.timelimiter.TimeLimiterOperator;
import io.github.resilience4j.timelimiter.TimeLimiter;
import io.github.resilience4j.timelimiter.TimeLimiterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * Resolves client data through Kafka request-reply and caches master data in Redis.
 */
@Service
@Slf4j
public class ClientLookupService {

    private static final Duration CACHE_TTL = Duration.ofMinutes(30);

    private final ReplyingKafkaTemplate<String, ClientValidationRequest, String>
        replyingKafkaTemplate;
    private final ReactiveRedisTemplate<String, ClientSummary> clientSummaryRedisTemplate;
    private final CircuitBreaker clientServiceCircuitBreaker;
    private final TimeLimiter clientServiceTimeLimiter;
    private final String requestTopic;
    private final ObjectMapper objectMapper;

    public ClientLookupService(
            ReplyingKafkaTemplate<String, ClientValidationRequest, String> replyingKafkaTemplate,
            ReactiveRedisTemplate<String, ClientSummary> clientSummaryRedisTemplate,
            CircuitBreakerRegistry circuitBreakerRegistry,
            TimeLimiterRegistry timeLimiterRegistry,
            ObjectMapper objectMapper,
            @Value("${topics.client-validation-request}") String requestTopic) {
        this.replyingKafkaTemplate = replyingKafkaTemplate;
        this.clientSummaryRedisTemplate = clientSummaryRedisTemplate;
        this.clientServiceCircuitBreaker = circuitBreakerRegistry.circuitBreaker("clientServiceCircuitBreaker");
        this.clientServiceTimeLimiter = timeLimiterRegistry.timeLimiter("clientServiceCircuitBreaker");
        this.objectMapper = objectMapper;
        this.requestTopic = requestTopic;
    }

    public Mono<ClientSummary> getClientById(String clientId) {
        if (clientId == null || clientId.trim().isEmpty()) {
            return Mono.error(new BusinessException("Client ID is required", "INVALID_CLIENT_ID"));
        }

        return clientSummaryRedisTemplate.opsForValue().get(cacheKey(clientId))
            .switchIfEmpty(fetchClientSummary(clientId)
                .flatMap(clientSummary -> clientSummaryRedisTemplate.opsForValue()
                    .set(cacheKey(clientId), clientSummary, CACHE_TTL)
                    .thenReturn(clientSummary)))
            .transformDeferred(CircuitBreakerOperator.of(clientServiceCircuitBreaker))
            .transformDeferred(TimeLimiterOperator.of(clientServiceTimeLimiter))
            .doOnNext(client -> log.debug("Validated client {} with type {}", client.getId(), client.getClientType()))
            .onErrorResume(TimeoutException.class, error -> fallbackClientLookup(clientId, error))
            .onErrorResume(CallNotPermittedException.class, error -> fallbackClientLookup(clientId, error));
    }

    private Mono<ClientSummary> fetchClientSummary(String clientId) {
        return Mono.fromCallable(() -> {
            String requestId = UUID.randomUUID().toString();
            ClientValidationRequest request = new ClientValidationRequest(requestId, clientId);
            log.info("Sending Kafka client validation requestId={} clientId={}", requestId, clientId);
            ProducerRecord<String, ClientValidationRequest> record =
                new ProducerRecord<>(requestTopic, clientId, request);
            RequestReplyFuture<String, ClientValidationRequest, String> replyFuture =
                replyingKafkaTemplate.sendAndReceive(record);

            ConsumerRecord<String, String> responseRecord = replyFuture.get();
            String responsePayload = responseRecord.value();
            if (responsePayload == null || responsePayload.isBlank()) {
                throw new BusinessException("Client validation returned an empty response", "CLIENT_VALIDATION_ERROR");
            }
            ClientValidationResponse response = objectMapper.readValue(responsePayload, ClientValidationResponse.class);
            log.info("Received Kafka client validation response requestId={} clientId={} found={}",
                response.getRequestId(), response.getClientId(), response.isFound());
            if (!response.isFound()) {
                throw new BusinessException(
                    response.getErrorMessage() == null ? "Client not found" : response.getErrorMessage(),
                    response.getErrorCode() == null ? "CLIENT_NOT_FOUND" : response.getErrorCode()
                );
            }

            return new ClientSummary(response.getClientId(), response.getClientType(), response.getProfile());
        })
            .subscribeOn(Schedulers.boundedElastic())
            .onErrorMap(ExecutionException.class, error -> new BusinessException(
                "Client validation request failed",
                "CLIENT_VALIDATION_ERROR"
            ));
    }

    private String cacheKey(String clientId) {
        return "client-summary::" + clientId;
    }

    private Mono<ClientSummary> fallbackClientLookup(String clientId, Throwable error) {
        log.warn("Client lookup fallback triggered for client {}: {}", clientId, error.getMessage());
        return Mono.error(new BusinessException(
            "Client validation is temporarily unavailable after a 2-second timeout or open circuit",
            "CLIENT_SERVICE_RESILIENCE_TRIGGERED"
        ));
    }
}
