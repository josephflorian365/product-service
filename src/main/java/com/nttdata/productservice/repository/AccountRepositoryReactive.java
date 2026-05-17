package com.nttdata.productservice.repository;

import com.nttdata.productservice.model.Account;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Reactive MongoDB Repository for Account entity.
 * Non-blocking operations using Project Reactor.
 *
 * All operations return Mono or Flux for reactive processing.
 */
@Repository
public interface AccountRepositoryReactive extends ReactiveMongoRepository<Account, String> {

    /**
     * Find all accounts for a specific client reactively.
     *
     * @param clientId the client ID
     * @return Flux of accounts for the client
     */
    Flux<Account> findByClientId(String clientId);

    /**
     * Find all accounts where a client is included as account holder reactively.
     *
     * @param holderId the holder client ID
     * @return Flux of accounts
     */
    Flux<Account> findByHoldersContaining(String holderId);

    /**
     * Find accounts by client ID and account type reactively.
     *
     * @param clientId the client ID
     * @param accountType the account type
     * @return Flux of accounts matching criteria
     */
    Flux<Account> findByClientIdAndAccountType(String clientId, String accountType);

    /**
     * Count accounts by client ID reactively.
     *
     * @param clientId the client ID
     * @return Mono containing the count
     */
    Mono<Long> countByClientId(String clientId);

    /**
     * Count accounts by client ID and type reactively.
     *
     * @param clientId the client ID
     * @param accountType the account type
     * @return Mono containing the count
     */
    Mono<Long> countByClientIdAndAccountType(String clientId, String accountType);

    /**
     * Find accounts by client type reactively.
     *
     * @param clientType the client type (PERSONAL or EMPRESARIAL)
     * @return Flux of accounts for clients of this type
     */
    Flux<Account> findByClientType(String clientType);

    /**
     * Find accounts by account type reactively.
     *
     * @param accountType the account type
     * @return Flux of accounts of this type
     */
    Flux<Account> findByAccountType(String accountType);
}

