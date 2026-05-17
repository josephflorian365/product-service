package com.nttdata.productservice.repository;

import com.nttdata.productservice.model.Transaction;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

/**
 * Reactive MongoDB Repository for Transaction entity.
 * Non-blocking operations using Project Reactor.
 *
 * All operations return Mono or Flux for reactive processing.
 */
@Repository
public interface TransactionRepositoryReactive extends ReactiveMongoRepository<Transaction, String> {

    /**
     * Find all transactions for a specific product reactively.
     * Ordered by date descending (most recent first).
     *
     * @param productId the product ID (Account or Credit)
     * @return Flux of transactions ordered by date descending
     */
    Flux<Transaction> findByProductIdOrderByDateDesc(String productId);

    /**
     * Find the latest 10 transactions for a product reactively.
     *
     * @param productId the product ID
     * @return Flux of top 10 transactions ordered by date descending
     */
    Flux<Transaction> findTop10ByProductIdOrderByDateDesc(String productId);

    /**
     * Find transactions by product ID reactively.
     *
     * @param productId the product ID
     * @return Flux of transactions
     */
    Flux<Transaction> findByProductId(String productId);

    /**
     * Find transactions by product type reactively.
     *
     * @param productType the product type (ACCOUNT or CREDIT)
     * @return Flux of transactions of this type
     */
    Flux<Transaction> findByProductType(String productType);

    /**
     * Find transactions by transaction type reactively.
     *
     * @param transactionType the transaction type (DEPOSIT, WITHDRAWAL, etc.)
     * @return Flux of transactions of this type
     */
    Flux<Transaction> findByTransactionType(String transactionType);

    /**
     * Count transactions by product ID reactively.
     *
     * @param productId the product ID
     * @return Mono containing the count
     */
    Mono<Long> countByProductId(String productId);

    /**
     * Count transactions by transaction type reactively.
     *
     * @param transactionType the transaction type
     * @return Mono containing the count
     */
    Mono<Long> countByTransactionType(String transactionType);

    /**
     * Count transactions for a product within a time range reactively.
     *
     * @param productId the product ID
     * @param startDate range start
     * @param endDate range end
     * @return Mono containing the count
     */
    Mono<Long> countByProductIdAndDateBetween(String productId, LocalDateTime startDate, LocalDateTime endDate);

    /**
     * Find transactions by product type and transaction type reactively.
     *
     * @param productType the product type
     * @param transactionType the transaction type
     * @return Flux of matching transactions
     */
    Flux<Transaction> findByProductTypeAndTransactionType(String productType, String transactionType);

    /**
     * Find transactions by product type and date range reactively.
     *
     * @param productType the product type
     * @param startDate the interval start
     * @param endDate the interval end
     * @return Flux of matching transactions ordered by date descending
     */
    Flux<Transaction> findByProductTypeAndDateBetweenOrderByDateDesc(
        String productType,
        LocalDateTime startDate,
        LocalDateTime endDate
    );
}

