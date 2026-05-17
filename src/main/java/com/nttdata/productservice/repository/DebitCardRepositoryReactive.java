package com.nttdata.productservice.repository;

import com.nttdata.productservice.model.DebitCard;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
public interface DebitCardRepositoryReactive extends ReactiveMongoRepository<DebitCard, String> {

    Flux<DebitCard> findByClientId(String clientId);

    Mono<DebitCard> findByCardNumber(String cardNumber);

    Mono<Boolean> existsByCardNumber(String cardNumber);
}
