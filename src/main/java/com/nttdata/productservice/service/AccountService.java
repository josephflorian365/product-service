package com.nttdata.productservice.service;

import com.nttdata.productservice.exception.BusinessException;
import com.nttdata.productservice.model.Account;
import com.nttdata.productservice.model.ClientSummary;
import com.nttdata.productservice.model.ProductBalance;
import com.nttdata.productservice.repository.AccountRepositoryReactive;
import com.nttdata.productservice.repository.CreditRepositoryReactive;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class AccountService {

    private static final BigDecimal ZERO = BigDecimal.ZERO;

    private final AccountRepositoryReactive accountRepository;
    private final CreditRepositoryReactive creditRepository;
    private final ClientLookupService clientLookupService;
    private final ProductEligibilityService productEligibilityService;

    public Single<Account> createAccount(Account account) {
        log.info("Creating account reactively for client {}", account.getClientId());

        return Mono.just(account)
            .doOnNext(this::validateAccountCreation)
            .flatMap(this::enrichAndValidateAccount)
            .map(acc -> {
                acc.setCreatedDate(LocalDateTime.now());
                acc.setCurrentMonthMovements(0);
                acc.setLastMovementDate(LocalDateTime.now());
                return acc;
            })
            .flatMap(accountRepository::save)
            .as(Single::fromPublisher);
    }

    public Flowable<Account> getAllAccounts() {
        return Flowable.fromPublisher(accountRepository.findAll());
    }

    public Maybe<Account> getAccountById(String id) {
        return Maybe.fromPublisher(accountRepository.findById(id));
    }

    public Single<Account> updateAccount(String id, Account account) {
        return accountRepository.findById(id)
            .switchIfEmpty(Mono.error(new BusinessException("Account not found", "ACCOUNT_NOT_FOUND")))
            .flatMap(existing -> {
                validateAccountCreation(account);
                return enrichAndValidateAccount(account)
                    .map(validated -> {
                        validated.setId(id);
                        validated.setCreatedDate(existing.getCreatedDate());
                        validated.setCurrentMonthMovements(existing.getCurrentMonthMovements());
                        validated.setLastMovementDate(existing.getLastMovementDate());
                        return validated;
                    });
            })
            .flatMap(accountRepository::save)
            .as(Single::fromPublisher);
    }

    public Completable deleteAccount(String id) {
        return accountRepository.findById(id)
            .switchIfEmpty(Mono.error(new BusinessException("Account not found", "ACCOUNT_NOT_FOUND")))
            .flatMap(existing -> accountRepository.deleteById(existing.getId()))
            .then()
            .as(Completable::fromPublisher);
    }

    public Flowable<Account> getAccountsByClientId(String clientId) {
        return Flowable.fromPublisher(getAccountsForClient(clientId));
    }

    public Maybe<Account> getAccountByClientAndId(String clientId, String accountId) {
        return Maybe.fromPublisher(accountRepository.findById(accountId)
            .filter(account -> isClientOwner(account, clientId))
            .switchIfEmpty(Mono.error(new BusinessException(
                "Account does not belong to the specified client",
                "ACCOUNT_NOT_OWNED_BY_CLIENT"))));
    }

    public Flowable<ProductBalance> getAccountBalancesByClientId(String clientId) {
        return Flowable.fromPublisher(getAccountsForClient(clientId)
            .map(this::toAccountBalance));
    }

    private Mono<Account> enrichAndValidateAccount(Account account) {
        return clientLookupService.getClientById(account.getClientId())
            .flatMap(client -> productEligibilityService.validateNoOverdueDebt(account.getClientId())
                .then(Mono.defer(() -> {
                    account.setClientType(client.getClientType());
                    return validateAccountRules(account, client);
                })));
    }

    private Mono<Account> validateAccountRules(Account account, ClientSummary client) {
        String clientType = client.getClientType().toUpperCase();
        String accountType = account.getAccountType().toUpperCase();
        String profile = client.getProfile() == null ? "" : client.getProfile().toUpperCase();

        if ("PERSONAL".equals(clientType)) {
            normalizePersonalOwnership(account);
            if ("SAVINGS".equals(accountType) || "CURRENT".equals(accountType)) {
                return accountRepository.countByClientIdAndAccountType(account.getClientId(), accountType)
                    .flatMap(count -> count > 0
                        ? Mono.error(new BusinessException(
                            "Personal clients can have only one " + accountType + " account",
                            "MAX_" + accountType + "_ACCOUNTS_EXCEEDED"))
                        : validateProfileAndMaintenanceRules(account, clientType, profile));
            }
            if ("FIXED_TERM".equals(accountType)) {
                return validateProfileAndMaintenanceRules(account, clientType, profile);
            }
            return Mono.error(new BusinessException("Invalid account type: " + accountType, "INVALID_ACCOUNT_TYPE"));
        }

        if ("EMPRESARIAL".equals(clientType)) {
            normalizeBusinessOwnership(account);
            if ("SAVINGS".equals(accountType) || "FIXED_TERM".equals(accountType)) {
                return Mono.error(new BusinessException(
                    "Business clients cannot have " + accountType + " accounts",
                    "INVALID_ACCOUNT_TYPE_FOR_BUSINESS"));
            }
            if ("CURRENT".equals(accountType)) {
                return validateProfileAndMaintenanceRules(account, clientType, profile);
            }
            return Mono.error(new BusinessException("Invalid account type: " + accountType, "INVALID_ACCOUNT_TYPE"));
        }

        return Mono.error(new BusinessException("Invalid client type: " + clientType, "INVALID_CLIENT_TYPE"));
    }

    private void validateAccountCreation(Account account) {
        if (account.getClientType() == null || account.getClientType().trim().isEmpty()) {
            throw new BusinessException("Client type is required", "INVALID_CLIENT_TYPE");
        }
        if (account.getAccountType() == null || account.getAccountType().trim().isEmpty()) {
            throw new BusinessException("Account type is required", "INVALID_ACCOUNT_TYPE");
        }
        if (account.getBalance() == null) {
            throw new BusinessException("Initial balance is required", "INVALID_INITIAL_BALANCE");
        }
        if (account.getBalance() != null && account.getMinimumOpeningAmount() != null
            && account.getBalance().compareTo(account.getMinimumOpeningAmount()) < 0) {
            throw new BusinessException(
                "Initial balance must be at least " + account.getMinimumOpeningAmount(),
                "INSUFFICIENT_OPENING_BALANCE"
            );
        }
        if (account.getMinimumOpeningAmount() != null && account.getMinimumOpeningAmount().compareTo(ZERO) < 0) {
            throw new BusinessException(
                "Minimum opening amount cannot be negative",
                "INVALID_MINIMUM_OPENING_AMOUNT");
        }
    }

    private void normalizePersonalOwnership(Account account) {
        account.setHolders(List.of(account.getClientId()));
        account.setAuthorizedSigners(new ArrayList<>());
    }

    private void normalizeBusinessOwnership(Account account) {
        List<String> normalizedHolders = normalizeParticipants(account.getHolders(), true);
        if (!normalizedHolders.contains(account.getClientId())) {
            normalizedHolders.add(0, account.getClientId());
            normalizedHolders = normalizeParticipants(normalizedHolders, true);
        }
        account.setHolders(normalizedHolders);
        account.setAuthorizedSigners(normalizeParticipants(account.getAuthorizedSigners(), false));
    }

    private List<String> normalizeParticipants(List<String> rawValues, boolean required) {
        List<String> normalized = rawValues == null
            ? new ArrayList<>()
            : rawValues.stream()
                .filter(Objects::nonNull)
                .map(String::trim)
                .filter(value -> !value.isEmpty())
                .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);

        normalized = new ArrayList<>(new LinkedHashSet<>(normalized));

        if (required && normalized.isEmpty()) {
            throw new BusinessException(
                "Business current accounts require at least one account holder",
                "MISSING_ACCOUNT_HOLDERS");
        }

        return normalized;
    }

    private reactor.core.publisher.Flux<Account> getAccountsForClient(String clientId) {
        return accountRepository.findByClientId(clientId)
            .concatWith(accountRepository.findByHoldersContaining(clientId))
            .collect(Collectors.toMap(Account::getId, account -> account, (left, right) -> left))
            .flatMapMany(map -> reactor.core.publisher.Flux.fromIterable(map.values()))
            .sort(
                Comparator.comparing(
                    Account::getCreatedDate,
                    Comparator.nullsLast(Comparator.naturalOrder())
                ).reversed()
            );
    }

    private boolean isClientOwner(Account account, String clientId) {
        if (clientId.equals(account.getClientId())) {
            return true;
        }
        return account.getHolders() != null && account.getHolders().contains(clientId);
    }

    private ProductBalance toAccountBalance(Account account) {
        return new ProductBalance(
            account.getId(),
            "ACCOUNT",
            account.getAccountType(),
            account.getBalance(),
            account.getBalance()
        );
    }

    private Mono<Account> validateProfileAndMaintenanceRules(Account account, String clientType, String profile) {
        String accountType = account.getAccountType().toUpperCase();

        if ("SAVINGS".equals(accountType) || "FIXED_TERM".equals(accountType)) {
            if (account.getMaintenanceFee() != null && account.getMaintenanceFee().compareTo(ZERO) > 0) {
                return Mono.error(new BusinessException(
                    accountType + " accounts cannot have maintenance fees",
                    "INVALID_MAINTENANCE_FEE"));
            }
            account.setMaintenanceFee(ZERO);
        }

        if ("CURRENT".equals(accountType) && account.getMaintenanceFee() == null) {
            return Mono.error(new BusinessException(
                "Current accounts require a maintenance fee value",
                "MISSING_MAINTENANCE_FEE"));
        }

        if ("CURRENT".equals(accountType) && account.getMaintenanceFee() != null
            && account.getMaintenanceFee().compareTo(ZERO) < 0) {
            return Mono.error(new BusinessException(
                "Maintenance fee cannot be negative",
                "INVALID_MAINTENANCE_FEE"));
        }

        if ("VIP".equals(profile) && "PERSONAL".equals(clientType) && "SAVINGS".equals(accountType)) {
            if (account.getMinimumDailyAverage() == null || account.getMinimumDailyAverage().compareTo(ZERO) <= 0) {
                return Mono.error(new BusinessException(
                    "VIP savings accounts require a positive minimum daily average",
                    "MISSING_MINIMUM_DAILY_AVERAGE"));
            }
            return validateCreditCardRequirement(account, "VIP savings accounts require an existing credit card");
        }

        if ("PYME".equals(profile) && "EMPRESARIAL".equals(clientType) && "CURRENT".equals(accountType)) {
            if (account.getMaintenanceFee().compareTo(ZERO) != 0) {
                return Mono.error(new BusinessException(
                    "PYME current accounts must not charge maintenance fees",
                    "INVALID_PYME_MAINTENANCE_FEE"));
            }
            return validateCreditCardRequirement(account, "PYME current accounts require an existing credit card");
        }

        return Mono.just(account);
    }

    private Mono<Account> validateCreditCardRequirement(Account account, String message) {
        return creditRepository.existsByClientIdAndCreditType(account.getClientId(), "CREDIT_CARD")
            .flatMap(hasCreditCard -> hasCreditCard
                ? Mono.just(account)
                : Mono.error(new BusinessException(message, "MISSING_REQUIRED_CREDIT_CARD")));
    }
}
