package com.nttdata.productservice.service;

import com.nttdata.productservice.exception.BusinessException;
import com.nttdata.productservice.model.DebitCard;
import com.nttdata.productservice.model.Transaction;
import com.nttdata.productservice.model.YankiDebitCardLinkRequest;
import com.nttdata.productservice.model.YankiPaymentRequest;
import com.nttdata.productservice.model.YankiPaymentResponse;
import com.nttdata.productservice.model.YankiWallet;
import com.nttdata.productservice.messaging.DomainEventPublisher;
import com.nttdata.productservice.repository.DebitCardRepositoryReactive;
import com.nttdata.productservice.repository.YankiWalletRepositoryReactive;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.regex.Pattern;

@Service
@RequiredArgsConstructor
public class YankiWalletService {

    private static final BigDecimal ZERO = BigDecimal.ZERO;
    private static final Pattern EMAIL_PATTERN =
        Pattern.compile("^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+$");

    private final YankiWalletRepositoryReactive yankiWalletRepository;
    private final DebitCardRepositoryReactive debitCardRepository;
    private final TransactionService transactionService;
    private final DomainEventPublisher domainEventPublisher;

    public Single<YankiWallet> createWallet(YankiWallet wallet) {
        return Mono.just(wallet)
            .doOnNext(this::validateWalletData)
            .flatMap(this::validateUniqueness)
            .map(valid -> {
                valid.setCreatedDate(LocalDateTime.now());
                return valid;
            })
            .flatMap(yankiWalletRepository::save)
            .doOnSuccess(saved -> domainEventPublisher.publish(
                "YANKI_WALLET_CREATED",
                "YANKI_WALLET",
                saved.getId(),
                java.util.Map.of(
                    "phoneNumber", saved.getPhoneNumber(),
                    "documentNumber", saved.getDocumentNumber()
                )))
            .as(Single::fromPublisher);
    }

    public Flowable<YankiWallet> getAllWallets() {
        return Flowable.fromPublisher(yankiWalletRepository.findAll());
    }

    public Maybe<YankiWallet> getWalletById(String id) {
        return Maybe.fromPublisher(yankiWalletRepository.findById(id));
    }

    public Single<YankiWallet> updateWallet(String id, YankiWallet wallet) {
        return yankiWalletRepository.findById(id)
            .switchIfEmpty(Mono.error(new BusinessException("Yanki wallet not found", "YANKI_WALLET_NOT_FOUND")))
            .doOnNext(existing -> validateWalletData(wallet))
            .flatMap(existing -> validateWalletUniquenessForUpdate(id, wallet)
                .map(valid -> mergeWallet(existing, valid)))
            .flatMap(yankiWalletRepository::save)
            .doOnSuccess(saved -> domainEventPublisher.publish(
                "YANKI_WALLET_UPDATED",
                "YANKI_WALLET",
                saved.getId(),
                java.util.Map.of("phoneNumber", saved.getPhoneNumber())))
            .as(Single::fromPublisher);
    }

    public Single<Boolean> deleteWallet(String id) {
        return yankiWalletRepository.findById(id)
            .switchIfEmpty(Mono.error(new BusinessException("Yanki wallet not found", "YANKI_WALLET_NOT_FOUND")))
            .flatMap(existing -> yankiWalletRepository.deleteById(existing.getId()).thenReturn(Boolean.TRUE))
            .as(Single::fromPublisher);
    }

    public Single<YankiWallet> linkDebitCard(String walletId, YankiDebitCardLinkRequest request) {
        if (request.getDebitCardId() == null || request.getDebitCardId().trim().isEmpty()) {
            return Single.error(new BusinessException("Debit card ID is required", "INVALID_DEBIT_CARD_ID"));
        }

        return yankiWalletRepository.findById(walletId)
            .switchIfEmpty(Mono.error(new BusinessException("Yanki wallet not found", "YANKI_WALLET_NOT_FOUND")))
            .flatMap(wallet -> debitCardRepository.findById(request.getDebitCardId())
                .switchIfEmpty(Mono.error(new BusinessException("Debit card not found", "DEBIT_CARD_NOT_FOUND")))
                .flatMap(debitCard -> {
                    wallet.setLinkedDebitCardId(debitCard.getId());
                    return yankiWalletRepository.save(wallet);
                }))
            .doOnSuccess(saved -> domainEventPublisher.publish(
                "YANKI_WALLET_LINKED_TO_DEBIT_CARD",
                "YANKI_WALLET",
                saved.getId(),
                java.util.Map.of("linkedDebitCardId", saved.getLinkedDebitCardId())))
            .as(Single::fromPublisher);
    }

    public Single<YankiPaymentResponse> sendPayment(YankiPaymentRequest request) {
        return validatePaymentRequest(request)
            .flatMap(valid -> yankiWalletRepository.findByPhoneNumber(valid.getSourcePhoneNumber())
                .switchIfEmpty(Mono.error(new BusinessException("Source wallet not found", "YANKI_WALLET_NOT_FOUND")))
                .flatMap(source -> yankiWalletRepository.findByPhoneNumber(valid.getDestinationPhoneNumber())
                    .switchIfEmpty(Mono.error(new BusinessException(
                        "Destination wallet not found",
                        "YANKI_WALLET_NOT_FOUND")))
                    .flatMap(destination -> executeWalletPayment(valid, source, destination))))
            .doOnSuccess(response -> domainEventPublisher.publish(
                "YANKI_PAYMENT_SENT",
                "YANKI_PAYMENT",
                response.getReference(),
                java.util.Map.of(
                    "sourcePhoneNumber", request.getSourcePhoneNumber(),
                    "destinationPhoneNumber", request.getDestinationPhoneNumber(),
                    "amount", request.getAmount()
                )))
            .as(Single::fromPublisher);
    }

    private Mono<YankiWallet> validateUniqueness(YankiWallet wallet) {
        return yankiWalletRepository.existsByDocumentNumber(wallet.getDocumentNumber())
            .flatMap(documentExists -> documentExists
                ? Mono.error(new BusinessException("Document number already exists", "DUPLICATE_DOCUMENT"))
                : yankiWalletRepository.existsByPhoneNumber(wallet.getPhoneNumber()))
            .flatMap(phoneExists -> phoneExists
                ? Mono.error(new BusinessException("Phone number already exists", "DUPLICATE_PHONE"))
                : yankiWalletRepository.existsByImei(wallet.getImei()))
            .flatMap(imeiExists -> imeiExists
                ? Mono.error(new BusinessException("IMEI already exists", "DUPLICATE_IMEI"))
                : Mono.just(wallet));
    }

    private Mono<YankiWallet> validateWalletUniquenessForUpdate(String walletId, YankiWallet wallet) {
        return yankiWalletRepository.findByDocumentNumber(wallet.getDocumentNumber())
            .flatMap(existing -> existing.getId().equals(walletId)
                ? Mono.empty()
                : Mono.error(new BusinessException("Document number already exists", "DUPLICATE_DOCUMENT")))
            .then(yankiWalletRepository.findByPhoneNumber(wallet.getPhoneNumber())
                .flatMap(existing -> existing.getId().equals(walletId)
                    ? Mono.empty()
                    : Mono.error(new BusinessException("Phone number already exists", "DUPLICATE_PHONE"))))
            .then(yankiWalletRepository.findByImei(wallet.getImei())
                .flatMap(existing -> existing.getId().equals(walletId)
                    ? Mono.empty()
                    : Mono.error(new BusinessException("IMEI already exists", "DUPLICATE_IMEI"))))
            .thenReturn(wallet);
    }

    private void validateWalletData(YankiWallet wallet) {
        if (wallet.getDocumentType() == null || wallet.getDocumentType().trim().isEmpty()) {
            throw new BusinessException("Document type is required", "INVALID_DOCUMENT_TYPE");
        }
        if (wallet.getDocumentNumber() == null || wallet.getDocumentNumber().trim().isEmpty()) {
            throw new BusinessException("Document number is required", "INVALID_DOCUMENT_NUMBER");
        }
        if (wallet.getPhoneNumber() == null || wallet.getPhoneNumber().trim().isEmpty()) {
            throw new BusinessException("Phone number is required", "INVALID_PHONE");
        }
        if (wallet.getImei() == null || wallet.getImei().trim().isEmpty()) {
            throw new BusinessException("IMEI is required", "INVALID_IMEI");
        }
        if (wallet.getEmail() == null || wallet.getEmail().trim().isEmpty()) {
            throw new BusinessException("Email is required", "INVALID_EMAIL");
        }
        if (!EMAIL_PATTERN.matcher(wallet.getEmail()).matches()) {
            throw new BusinessException("Invalid email format", "INVALID_EMAIL");
        }
    }

    private Mono<YankiPaymentRequest> validatePaymentRequest(YankiPaymentRequest request) {
        if (request.getSourcePhoneNumber() == null || request.getSourcePhoneNumber().trim().isEmpty()) {
            return Mono.error(new BusinessException("Source phone number is required", "INVALID_PHONE"));
        }
        if (request.getDestinationPhoneNumber() == null || request.getDestinationPhoneNumber().trim().isEmpty()) {
            return Mono.error(new BusinessException("Destination phone number is required", "INVALID_PHONE"));
        }
        if (request.getSourcePhoneNumber().equals(request.getDestinationPhoneNumber())) {
            return Mono.error(new BusinessException(
                "Source and destination phone numbers must be different",
                "INVALID_YANKI_PAYMENT"));
        }
        if (request.getAmount() == null || request.getAmount().compareTo(ZERO) <= 0) {
            return Mono.error(new BusinessException("Payment amount must be greater than zero", "INVALID_AMOUNT"));
        }
        return Mono.just(request);
    }

    private Mono<YankiPaymentResponse> executeWalletPayment(
            YankiPaymentRequest request,
            YankiWallet source,
            YankiWallet destination) {
        if (source.getLinkedDebitCardId() == null || source.getLinkedDebitCardId().trim().isEmpty()) {
            return Mono.error(new BusinessException(
                "Source wallet must be linked to a debit card",
                "YANKI_WALLET_NOT_LINKED"));
        }
        if (destination.getLinkedDebitCardId() == null || destination.getLinkedDebitCardId().trim().isEmpty()) {
            return Mono.error(new BusinessException(
                "Destination wallet must be linked to a debit card",
                "YANKI_WALLET_NOT_LINKED"));
        }

        String reference = "YNK-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();

        return debitCardRepository.findById(source.getLinkedDebitCardId())
            .switchIfEmpty(Mono.error(new BusinessException("Source debit card not found", "DEBIT_CARD_NOT_FOUND")))
            .flatMap(sourceCard -> debitCardRepository.findById(destination.getLinkedDebitCardId())
                .switchIfEmpty(Mono.error(new BusinessException(
                    "Destination debit card not found",
                    "DEBIT_CARD_NOT_FOUND")))
                .flatMap(destinationCard -> createTransactions(
                    request,
                    source,
                    destination,
                    sourceCard,
                    destinationCard,
                    reference)));
    }

    private Mono<YankiPaymentResponse> createTransactions(
            YankiPaymentRequest request,
            YankiWallet source,
            YankiWallet destination,
            DebitCard sourceCard,
            DebitCard destinationCard,
            String reference) {
        Transaction debitTransaction = new Transaction();
        debitTransaction.setProductId(sourceCard.getPrimaryAccountId());
        debitTransaction.setProductType("ACCOUNT");
        debitTransaction.setTransactionType("WITHDRAWAL");
        debitTransaction.setAmount(request.getAmount());
        debitTransaction.setDescription(buildDescription(
            "Yanki payment to " + destination.getPhoneNumber(),
            request.getDescription()));
        debitTransaction.setReference(reference);

        Transaction creditTransaction = new Transaction();
        creditTransaction.setProductId(destinationCard.getPrimaryAccountId());
        creditTransaction.setProductType("ACCOUNT");
        creditTransaction.setTransactionType("DEPOSIT");
        creditTransaction.setAmount(request.getAmount());
        creditTransaction.setDescription(buildDescription(
            "Yanki payment from " + source.getPhoneNumber(),
            request.getDescription()));
        creditTransaction.setReference(reference);

        return transactionService.persistOwnedTransaction(debitTransaction)
            .flatMap(savedDebit -> transactionService.persistOwnedTransaction(creditTransaction)
                .map(savedCredit -> new YankiPaymentResponse(reference, savedDebit, savedCredit)));
    }

    private String buildDescription(String baseDescription, String customDescription) {
        if (customDescription == null || customDescription.trim().isEmpty()) {
            return baseDescription;
        }
        return baseDescription + " - " + customDescription.trim();
    }

    private YankiWallet mergeWallet(YankiWallet existing, YankiWallet incoming) {
        existing.setDocumentType(incoming.getDocumentType());
        existing.setDocumentNumber(incoming.getDocumentNumber());
        existing.setPhoneNumber(incoming.getPhoneNumber());
        existing.setImei(incoming.getImei());
        existing.setEmail(incoming.getEmail());
        if (incoming.getLinkedDebitCardId() != null && !incoming.getLinkedDebitCardId().trim().isEmpty()) {
            existing.setLinkedDebitCardId(incoming.getLinkedDebitCardId());
        }
        return existing;
    }
}
