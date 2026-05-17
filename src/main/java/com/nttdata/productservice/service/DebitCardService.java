package com.nttdata.productservice.service;

import com.nttdata.productservice.exception.BusinessException;
import com.nttdata.productservice.model.Account;
import com.nttdata.productservice.model.DebitCard;
import com.nttdata.productservice.messaging.DomainEventPublisher;
import com.nttdata.productservice.repository.AccountRepositoryReactive;
import com.nttdata.productservice.repository.DebitCardRepositoryReactive;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class DebitCardService {

    private final DebitCardRepositoryReactive debitCardRepository;
    private final AccountRepositoryReactive accountRepository;
    private final DomainEventPublisher domainEventPublisher;

    public Single<DebitCard> createDebitCard(DebitCard debitCard) {
        return validateDebitCard(debitCard)
            .map(valid -> {
                valid.setCreatedDate(LocalDateTime.now());
                if (valid.getCardNumber() == null || valid.getCardNumber().trim().isEmpty()) {
                    valid.setCardNumber(generateCardNumber());
                }
                return valid;
            })
            .flatMap(this::ensureUniqueCardNumber)
            .flatMap(debitCardRepository::save)
            .doOnSuccess(saved -> domainEventPublisher.publish(
                "DEBIT_CARD_CREATED",
                "DEBIT_CARD",
                saved.getId(),
                java.util.Map.of(
                    "clientId", saved.getClientId(),
                    "primaryAccountId", saved.getPrimaryAccountId(),
                    "cardNumber", saved.getCardNumber()
                )))
            .as(Single::fromPublisher);
    }

    public Flowable<DebitCard> getDebitCardsByClientId(String clientId) {
        return Flowable.fromPublisher(debitCardRepository.findByClientId(clientId));
    }

    public Flowable<DebitCard> getAllDebitCards() {
        return Flowable.fromPublisher(debitCardRepository.findAll());
    }

    public Maybe<DebitCard> getDebitCardById(String id) {
        return Maybe.fromPublisher(debitCardRepository.findById(id));
    }

    public Single<DebitCard> updateDebitCard(String id, DebitCard debitCard) {
        return debitCardRepository.findById(id)
            .switchIfEmpty(Mono.error(new BusinessException("Debit card not found", "DEBIT_CARD_NOT_FOUND")))
            .flatMap(existing -> validateDebitCard(debitCard)
                .map(valid -> mergeDebitCard(existing, valid)))
            .flatMap(this::ensureUniqueCardNumberForUpdate)
            .flatMap(debitCardRepository::save)
            .doOnSuccess(saved -> domainEventPublisher.publish(
                "DEBIT_CARD_UPDATED",
                "DEBIT_CARD",
                saved.getId(),
                java.util.Map.of(
                    "clientId", saved.getClientId(),
                    "primaryAccountId", saved.getPrimaryAccountId()
                )))
            .as(Single::fromPublisher);
    }

    public Single<Boolean> deleteDebitCard(String id) {
        return debitCardRepository.findById(id)
            .switchIfEmpty(Mono.error(new BusinessException("Debit card not found", "DEBIT_CARD_NOT_FOUND")))
            .flatMap(existing -> debitCardRepository.deleteById(existing.getId()).thenReturn(Boolean.TRUE))
            .as(Single::fromPublisher);
    }

    Mono<DebitCard> validateDebitCard(DebitCard debitCard) {
        if (debitCard.getClientId() == null || debitCard.getClientId().trim().isEmpty()) {
            return Mono.error(new BusinessException("Client ID is required", "INVALID_CLIENT_ID"));
        }
        if (debitCard.getPrimaryAccountId() == null || debitCard.getPrimaryAccountId().trim().isEmpty()) {
            return Mono.error(new BusinessException("Primary account ID is required", "INVALID_PRIMARY_ACCOUNT"));
        }

        return accountRepository.findById(debitCard.getPrimaryAccountId())
            .switchIfEmpty(Mono.error(new BusinessException("Primary account not found", "ACCOUNT_NOT_FOUND")))
            .flatMap(account -> validateOwnership(debitCard, account).thenReturn(debitCard));
    }

    private Mono<Void> validateOwnership(DebitCard debitCard, Account account) {
        boolean isOwner = debitCard.getClientId().equals(account.getClientId())
            || (account.getHolders() != null && account.getHolders().contains(debitCard.getClientId()));

        if (!isOwner) {
            return Mono.error(new BusinessException(
                "Primary account does not belong to the specified client",
                "ACCOUNT_NOT_OWNED_BY_CLIENT"));
        }

        return Mono.empty();
    }

    private Mono<DebitCard> ensureUniqueCardNumber(DebitCard debitCard) {
        return debitCardRepository.existsByCardNumber(debitCard.getCardNumber())
            .flatMap(exists -> exists
                ? Mono.error(new BusinessException("Debit card number already exists", "DUPLICATE_DEBIT_CARD"))
                : Mono.just(debitCard));
    }

    private Mono<DebitCard> ensureUniqueCardNumberForUpdate(DebitCard debitCard) {
        return debitCardRepository.findByCardNumber(debitCard.getCardNumber())
            .flatMap(existing -> existing.getId().equals(debitCard.getId())
                ? Mono.just(debitCard)
                : Mono.error(new BusinessException("Debit card number already exists", "DUPLICATE_DEBIT_CARD")))
            .switchIfEmpty(Mono.just(debitCard));
    }

    private DebitCard mergeDebitCard(DebitCard existing, DebitCard incoming) {
        existing.setClientId(incoming.getClientId());
        existing.setPrimaryAccountId(incoming.getPrimaryAccountId());
        existing.setCardNumber(incoming.getCardNumber() == null || incoming.getCardNumber().trim().isEmpty()
            ? existing.getCardNumber()
            : incoming.getCardNumber());
        return existing;
    }

    private String generateCardNumber() {
        return "DC-" + UUID.randomUUID().toString().substring(0, 12).replace("-", "").toUpperCase();
    }
}
