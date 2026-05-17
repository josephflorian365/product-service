package com.nttdata.productservice.repository;

import com.nttdata.productservice.model.Credit;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.LocalDate;

/**
 * Reactive MongoDB Repository for Credit entity.
 * Non-blocking operations using Project Reactor.
 *
 * All operations return Mono or Flux for reactive processing.
 */
@Repository
public interface CreditRepositoryReactive extends ReactiveMongoRepository<Credit, String> {

    /**
     * Find all credits for a specific client reactively.
     *
     * @param clientId the client ID
     * @return Flux of credits for the client
     */
    Flux<Credit> findByClientId(String clientId);

    /**
     * Find credits by client ID and credit type reactively.
     *
     * @param clientId the client ID
     * @param creditType the credit type
     * @return Flux of credits matching criteria
     */
    Flux<Credit> findByClientIdAndCreditType(String clientId, String creditType);

    /**
     * Count credits by client ID reactively.
     *
     * @param clientId the client ID
     * @return Mono containing the count
     */
    Mono<Long> countByClientId(String clientId);

    /**
     * Count credits by client ID and type reactively.
     *
     * @param clientId the client ID
     * @param creditType the credit type
     * @return Mono containing the count
     */
    Mono<Long> countByClientIdAndCreditType(String clientId, String creditType);

    /**
     * Find credits by credit type reactively.
     *
     * @param creditType the credit type (PERSONAL, BUSINESS, CREDIT_CARD)
     * @return Flux of credits of this type
     */
    Flux<Credit> findByCreditType(String creditType);

    /**
     * Check if client has a credit of specific type reactively.
     *
     * @param clientId the client ID
     * @param creditType the credit type
     * @return Mono containing true if exists, false otherwise
     */
    Mono<Boolean> existsByClientIdAndCreditType(String clientId, String creditType);

    /**
     * Check whether a client has overdue debt on any credit product.
     *
     * @param clientId the client ID
     * @param outstandingBalance minimum outstanding balance
     * @param dueDate credits due before this date are considered overdue
     * @return Mono containing true if overdue debt exists
     */
    Mono<Boolean> existsByClientIdAndOutstandingBalanceGreaterThanAndDueDateBefore(
        String clientId,
        BigDecimal outstandingBalance,
        LocalDate dueDate
    );
}

