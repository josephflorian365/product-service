package com.nttdata.productservice.repository;

import com.nttdata.productservice.model.YankiWallet;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

@Repository
public interface YankiWalletRepositoryReactive extends ReactiveMongoRepository<YankiWallet, String> {

    Mono<YankiWallet> findByDocumentNumber(String documentNumber);

    Mono<YankiWallet> findByImei(String imei);

    Mono<Boolean> existsByDocumentNumber(String documentNumber);

    Mono<Boolean> existsByPhoneNumber(String phoneNumber);

    Mono<Boolean> existsByImei(String imei);

    Mono<YankiWallet> findByPhoneNumber(String phoneNumber);
}
