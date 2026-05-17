package com.nttdata.productservice.service;

import com.nttdata.productservice.exception.BusinessException;
import com.nttdata.productservice.model.ClientSummary;
import com.nttdata.productservice.model.Credit;
import com.nttdata.productservice.model.ProductBalance;
import com.nttdata.productservice.repository.CreditRepositoryReactive;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;

@Service
@RequiredArgsConstructor
@Slf4j
public class CreditService {

    private final CreditRepositoryReactive creditRepository;
    private final ClientLookupService clientLookupService;
    private final ProductEligibilityService productEligibilityService;

    public Single<Credit> createCredit(Credit credit) {
        log.info("Creating credit reactively for client {}", credit.getClientId());

        return Mono.just(credit)
            .doOnNext(this::validateCreditCreation)
            .flatMap(this::enrichAndValidateCredit)
            .map(this::initializeOutstandingBalance)
            .flatMap(creditRepository::save)
            .as(Single::fromPublisher);
    }

    public Flowable<Credit> getAllCredits() {
        return Flowable.fromPublisher(creditRepository.findAll());
    }

    public Maybe<Credit> getCreditById(String id) {
        return Maybe.fromPublisher(creditRepository.findById(id));
    }

    public Single<Credit> updateCredit(String id, Credit credit) {
        return creditRepository.findById(id)
            .switchIfEmpty(Mono.error(new BusinessException("Credit not found", "CREDIT_NOT_FOUND")))
            .flatMap(existing -> {
                validateCreditCreation(credit);
                return enrichAndValidateCredit(credit)
                    .map(validated -> {
                        validated.setId(id);
                        if (validated.getOutstandingBalance() == null) {
                            validated.setOutstandingBalance(existing.getOutstandingBalance());
                        }
                        return validated;
                    });
            })
            .flatMap(creditRepository::save)
            .as(Single::fromPublisher);
    }

    public Completable deleteCredit(String id) {
        return creditRepository.findById(id)
            .switchIfEmpty(Mono.error(new BusinessException("Credit not found", "CREDIT_NOT_FOUND")))
            .flatMap(existing -> creditRepository.deleteById(existing.getId()))
            .then()
            .as(Completable::fromPublisher);
    }

    public Flowable<Credit> getCreditsByClientId(String clientId) {
        return Flowable.fromPublisher(creditRepository.findByClientId(clientId));
    }

    public Maybe<Credit> getCreditByClientAndId(String clientId, String creditId) {
        return Maybe.fromPublisher(creditRepository.findById(creditId)
            .filter(credit -> clientId.equals(credit.getClientId()))
            .switchIfEmpty(Mono.error(new BusinessException(
                "Credit does not belong to the specified client",
                "CREDIT_NOT_OWNED_BY_CLIENT"))));
    }

    public Flowable<ProductBalance> getCreditBalancesByClientId(String clientId) {
        return Flowable.fromPublisher(creditRepository.findByClientId(clientId)
            .map(this::toCreditBalance));
    }

    private Mono<Credit> enrichAndValidateCredit(Credit credit) {
        return clientLookupService.getClientById(credit.getClientId())
            .flatMap(client -> productEligibilityService.validateNoOverdueDebt(credit.getClientId())
                .then(validateCreditRules(credit, client)));
    }

    private Mono<Credit> validateCreditRules(Credit credit, ClientSummary client) {
        String creditType = credit.getCreditType().toUpperCase();
        if ("PERSONAL".equals(creditType)) {
            if (!"PERSONAL".equalsIgnoreCase(client.getClientType())) {
                return Mono.error(new BusinessException(
                    "Only PERSONAL clients can acquire PERSONAL credits",
                    "INVALID_CREDIT_FOR_CLIENT_TYPE"));
            }
            return creditRepository.existsByClientIdAndCreditType(credit.getClientId(), creditType)
                .flatMap(exists -> exists
                    ? Mono.error(new BusinessException(
                        "Personal clients can have only one PERSONAL credit",
                        "MAX_PERSONAL_CREDITS_EXCEEDED"))
                    : Mono.just(credit));
        }
        if ("BUSINESS".equals(creditType)) {
            if (!"EMPRESARIAL".equalsIgnoreCase(client.getClientType())) {
                return Mono.error(new BusinessException(
                    "Only EMPRESARIAL clients can acquire BUSINESS credits",
                    "INVALID_CREDIT_FOR_CLIENT_TYPE"));
            }
            return Mono.just(credit);
        }
        if ("CREDIT_CARD".equals(creditType)) {
            return Mono.just(credit);
        }
        return Mono.error(new BusinessException("Invalid credit type: " + creditType, "INVALID_CREDIT_TYPE"));
    }

    private Credit initializeOutstandingBalance(Credit credit) {
        if (credit.getOutstandingBalance() != null) {
            return credit;
        }

        if ("CREDIT_CARD".equalsIgnoreCase(credit.getCreditType())) {
            credit.setOutstandingBalance(BigDecimal.ZERO);
        } else {
            credit.setOutstandingBalance(credit.getAmount());
        }
        return credit;
    }

    private ProductBalance toCreditBalance(Credit credit) {
        BigDecimal availableBalance = "CREDIT_CARD".equalsIgnoreCase(credit.getCreditType())
            ? credit.getCreditLimit().subtract(credit.getOutstandingBalance())
            : BigDecimal.ZERO;

        return new ProductBalance(
            credit.getId(),
            "CREDIT",
            credit.getCreditType(),
            credit.getOutstandingBalance(),
            availableBalance
        );
    }

    private void validateCreditCreation(Credit credit) {
        if (credit.getCreditType() == null || credit.getCreditType().trim().isEmpty()) {
            throw new BusinessException("Credit type is required", "INVALID_CREDIT_TYPE");
        }
        if (credit.getAmount() == null || credit.getAmount().signum() <= 0) {
            throw new BusinessException("Credit amount must be greater than zero", "INVALID_CREDIT_AMOUNT");
        }
        if (credit.getInterestRate() == null || credit.getInterestRate().signum() < 0) {
            throw new BusinessException("Interest rate cannot be negative", "INVALID_INTEREST_RATE");
        }
        if (credit.getInterestRate().compareTo(new BigDecimal("100")) > 0) {
            throw new BusinessException("Interest rate cannot exceed 100%", "INVALID_INTEREST_RATE");
        }
        if (credit.getTermMonths() == null || credit.getTermMonths() <= 0 || credit.getTermMonths() > 360) {
            throw new BusinessException("Term must be between 1 and 360 months", "INVALID_TERM_MONTHS");
        }
        if ("CREDIT_CARD".equalsIgnoreCase(credit.getCreditType())
            && (credit.getCreditLimit() == null || credit.getCreditLimit().signum() <= 0)) {
            throw new BusinessException(
                "Credit limit is required for credit cards and must be positive",
                "INVALID_CREDIT_LIMIT"
            );
        }
    }
}
