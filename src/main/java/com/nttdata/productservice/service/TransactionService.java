package com.nttdata.productservice.service;

import com.nttdata.productservice.exception.BusinessException;
import com.nttdata.productservice.model.Account;
import com.nttdata.productservice.model.Credit;
import com.nttdata.productservice.model.DebitCard;
import com.nttdata.productservice.model.DebitCardPaymentRequest;
import com.nttdata.productservice.model.Transaction;
import com.nttdata.productservice.model.ThirdPartyCreditPaymentRequest;
import com.nttdata.productservice.model.ThirdPartyCreditPaymentResponse;
import com.nttdata.productservice.model.TransferRequest;
import com.nttdata.productservice.model.TransferResponse;
import com.nttdata.productservice.messaging.DomainEventPublisher;
import com.nttdata.productservice.repository.AccountRepositoryReactive;
import com.nttdata.productservice.repository.CreditRepositoryReactive;
import com.nttdata.productservice.repository.DebitCardRepositoryReactive;
import com.nttdata.productservice.repository.TransactionRepositoryReactive;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.YearMonth;
import java.math.BigDecimal;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class TransactionService {

    private static final BigDecimal ZERO = BigDecimal.ZERO;

    private final TransactionRepositoryReactive transactionRepository;
    private final AccountRepositoryReactive accountRepository;
    private final CreditRepositoryReactive creditRepository;
    private final DebitCardRepositoryReactive debitCardRepository;
    private final DomainEventPublisher domainEventPublisher;

    public Single<Transaction> createTransaction(Transaction transaction) {
        log.info("Creating transaction reactively for product {}", transaction.getProductId());

        return validateTransaction(transaction)
            .map(valid -> {
                valid.setDate(LocalDateTime.now());
                return valid;
            })
            .flatMap(transactionRepository::save)
            .flatMap(saved -> applyTransaction(saved).thenReturn(saved))
            .as(Single::fromPublisher);
    }

    public Flowable<Transaction> getAllTransactions() {
        return Flowable.fromPublisher(transactionRepository.findAll());
    }

    public Maybe<Transaction> getTransactionById(String id) {
        return Maybe.fromPublisher(transactionRepository.findById(id));
    }

    public Single<Transaction> updateTransaction(String id, Transaction transaction) {
        return transactionRepository.findById(id)
            .switchIfEmpty(Mono.error(new BusinessException("Transaction not found", "TRANSACTION_NOT_FOUND")))
            .flatMap(existing -> {
                transaction.setId(id);
                transaction.setDate(existing.getDate());
                return transactionRepository.save(transaction);
            })
            .as(Single::fromPublisher);
    }

    public Completable deleteTransaction(String id) {
        return transactionRepository.findById(id)
            .switchIfEmpty(Mono.error(new BusinessException("Transaction not found", "TRANSACTION_NOT_FOUND")))
            .flatMap(existing -> transactionRepository.deleteById(existing.getId()))
            .then()
            .as(Completable::fromPublisher);
    }

    public Flowable<Transaction> getTransactionsByProductId(String productId) {
        return Flowable.fromPublisher(transactionRepository.findByProductIdOrderByDateDesc(productId));
    }

    public Flowable<Transaction> getTransactionsByClientAndProductId(
            String clientId,
            String productType,
            String productId) {
        return verifyClientOwnsProduct(clientId, productType, productId)
            .thenMany(transactionRepository.findByProductIdOrderByDateDesc(productId))
            .as(Flowable::fromPublisher);
    }

    public Single<TransferResponse> createTransfer(TransferRequest request) {
        return validateTransferRequest(request)
            .flatMap(valid -> accountRepository.findById(valid.getSourceAccountId())
                .switchIfEmpty(Mono.error(new BusinessException("Source account not found", "ACCOUNT_NOT_FOUND")))
                .flatMap(source -> verifyTransferOwnership(valid.getSourceClientId(), source)
                    .then(accountRepository.findById(valid.getDestinationAccountId())
                        .switchIfEmpty(Mono.error(new BusinessException(
                            "Destination account not found",
                            "ACCOUNT_NOT_FOUND")))
                        .flatMap(destination -> executeTransfer(valid, source, destination))))
            )
            .doOnSuccess(response -> domainEventPublisher.publish(
                "ACCOUNT_TRANSFER_CREATED",
                "TRANSFER",
                response.getReference(),
                java.util.Map.of(
                    "sourceAccountId", request.getSourceAccountId(),
                    "destinationAccountId", request.getDestinationAccountId(),
                    "amount", request.getAmount()
                )))
            .as(Single::fromPublisher);
    }

    public Single<ThirdPartyCreditPaymentResponse> createThirdPartyCreditPayment(
            ThirdPartyCreditPaymentRequest request) {
        return validateThirdPartyCreditPaymentRequest(request)
            .flatMap(valid -> accountRepository.findById(valid.getPayerAccountId())
                .switchIfEmpty(Mono.error(new BusinessException("Payer account not found", "ACCOUNT_NOT_FOUND")))
                .flatMap(account -> verifyTransferOwnership(valid.getPayerClientId(), account)
                    .then(creditRepository.findById(valid.getCreditId())
                        .switchIfEmpty(Mono.error(new BusinessException("Credit not found", "CREDIT_NOT_FOUND")))
                        .flatMap(credit -> executeThirdPartyCreditPayment(valid, account, credit))))
            )
            .doOnSuccess(response -> domainEventPublisher.publish(
                "THIRD_PARTY_CREDIT_PAYMENT_CREATED",
                "CREDIT_PAYMENT",
                response.getReference(),
                java.util.Map.of(
                    "payerAccountId", request.getPayerAccountId(),
                    "creditId", request.getCreditId(),
                    "amount", request.getAmount()
                )))
            .as(Single::fromPublisher);
    }

    public Single<Transaction> createDebitCardPayment(DebitCardPaymentRequest request) {
        return validateDebitCardPaymentRequest(request)
            .flatMap(valid -> debitCardRepository.findById(valid.getDebitCardId())
                .switchIfEmpty(Mono.error(new BusinessException("Debit card not found", "DEBIT_CARD_NOT_FOUND")))
                .flatMap(debitCard -> verifyDebitCardOwnership(valid.getClientId(), debitCard)
                    .then(executeDebitCardPayment(valid, debitCard))))
            .doOnSuccess(transaction -> domainEventPublisher.publish(
                "DEBIT_CARD_PAYMENT_CREATED",
                "DEBIT_CARD_PAYMENT",
                transaction.getReference(),
                java.util.Map.of(
                    "debitCardId", request.getDebitCardId(),
                    "clientId", request.getClientId(),
                    "amount", request.getAmount()
                )))
            .as(Single::fromPublisher);
    }

    private Mono<Transaction> validateTransaction(Transaction transaction) {
        if (transaction.getProductType() == null || transaction.getProductType().trim().isEmpty()) {
            return Mono.error(new BusinessException("Product type is required", "INVALID_PRODUCT_TYPE"));
        }
        if (transaction.getTransactionType() == null || transaction.getTransactionType().trim().isEmpty()) {
            return Mono.error(new BusinessException("Transaction type is required", "INVALID_TRANSACTION_TYPE"));
        }
        if (transaction.getAmount() == null || transaction.getAmount().signum() <= 0) {
            return Mono.error(new BusinessException("Transaction amount must be greater than zero", "INVALID_AMOUNT"));
        }

        String productType = transaction.getProductType().toUpperCase();
        if ("ACCOUNT".equals(productType)) {
            return accountRepository.findById(transaction.getProductId())
                .switchIfEmpty(Mono.error(new BusinessException("Account not found", "ACCOUNT_NOT_FOUND")))
                .flatMap(account -> validateTransactionAgainstAccount(transaction, account).thenReturn(transaction));
        }
        if ("CREDIT".equals(productType)) {
            return creditRepository.findById(transaction.getProductId())
                .switchIfEmpty(Mono.error(new BusinessException("Credit not found", "CREDIT_NOT_FOUND")))
                .flatMap(credit -> validateTransactionAgainstCredit(transaction, credit).thenReturn(transaction));
        }

        return Mono.error(new BusinessException("Invalid product type: " + transaction.getProductType(), "INVALID_PRODUCT_TYPE"));
    }

    private Mono<TransferRequest> validateTransferRequest(TransferRequest request) {
        if (request.getSourceClientId() == null || request.getSourceClientId().trim().isEmpty()) {
            return Mono.error(new BusinessException("Source client ID is required", "INVALID_CLIENT_ID"));
        }
        if (request.getSourceAccountId() == null || request.getSourceAccountId().trim().isEmpty()) {
            return Mono.error(new BusinessException("Source account ID is required", "INVALID_SOURCE_ACCOUNT"));
        }
        if (request.getDestinationAccountId() == null || request.getDestinationAccountId().trim().isEmpty()) {
            return Mono.error(new BusinessException(
                "Destination account ID is required",
                "INVALID_DESTINATION_ACCOUNT"));
        }
        if (request.getSourceAccountId().equals(request.getDestinationAccountId())) {
            return Mono.error(new BusinessException(
                "Source and destination accounts must be different",
                "INVALID_TRANSFER_ACCOUNTS"));
        }
        if (request.getAmount() == null || request.getAmount().compareTo(ZERO) <= 0) {
            return Mono.error(new BusinessException("Transfer amount must be greater than zero", "INVALID_AMOUNT"));
        }
        return Mono.just(request);
    }

    private Mono<ThirdPartyCreditPaymentRequest> validateThirdPartyCreditPaymentRequest(
            ThirdPartyCreditPaymentRequest request) {
        if (request.getPayerClientId() == null || request.getPayerClientId().trim().isEmpty()) {
            return Mono.error(new BusinessException("Payer client ID is required", "INVALID_CLIENT_ID"));
        }
        if (request.getPayerAccountId() == null || request.getPayerAccountId().trim().isEmpty()) {
            return Mono.error(new BusinessException("Payer account ID is required", "INVALID_SOURCE_ACCOUNT"));
        }
        if (request.getCreditId() == null || request.getCreditId().trim().isEmpty()) {
            return Mono.error(new BusinessException("Credit ID is required", "INVALID_CREDIT_ID"));
        }
        if (request.getAmount() == null || request.getAmount().compareTo(ZERO) <= 0) {
            return Mono.error(new BusinessException("Payment amount must be greater than zero", "INVALID_AMOUNT"));
        }
        return Mono.just(request);
    }

    private Mono<DebitCardPaymentRequest> validateDebitCardPaymentRequest(DebitCardPaymentRequest request) {
        if (request.getClientId() == null || request.getClientId().trim().isEmpty()) {
            return Mono.error(new BusinessException("Client ID is required", "INVALID_CLIENT_ID"));
        }
        if (request.getDebitCardId() == null || request.getDebitCardId().trim().isEmpty()) {
            return Mono.error(new BusinessException("Debit card ID is required", "INVALID_DEBIT_CARD_ID"));
        }
        if (request.getAmount() == null || request.getAmount().compareTo(ZERO) <= 0) {
            return Mono.error(new BusinessException("Payment amount must be greater than zero", "INVALID_AMOUNT"));
        }
        return Mono.just(request);
    }

    private Mono<Void> validateTransactionAgainstAccount(Transaction transaction, Account account) {
        String transactionType = transaction.getTransactionType().toUpperCase();
        String accountType = account.getAccountType().toUpperCase();

        if (!("DEPOSIT".equals(transactionType) || "WITHDRAWAL".equals(transactionType))) {
            return Mono.error(new BusinessException(
                "Accounts only support DEPOSIT and WITHDRAWAL transactions",
                "INVALID_ACCOUNT_TRANSACTION"));
        }

        if ("SAVINGS".equals(accountType)) {
            resetSavingsCounterIfNeeded(account);
            if (account.getMaxMonthlyMovements() != null
                && account.getCurrentMonthMovements() >= account.getMaxMonthlyMovements()) {
                return Mono.error(new BusinessException(
                    "Monthly movement limit of " + account.getMaxMonthlyMovements()
                        + " exceeded for SAVINGS account",
                    "MAX_MONTHLY_MOVEMENTS_EXCEEDED"));
            }
        }

        if ("FIXED_TERM".equals(accountType)) {
            return validateFixedTermTransaction(account)
                .then(applyTransactionFeePreview(transaction, account));
        }

        return applyTransactionFeePreview(transaction, account);
    }

    private void resetSavingsCounterIfNeeded(Account account) {
        if (account.getLastMovementDate() != null) {
            YearMonth lastMonth = YearMonth.from(account.getLastMovementDate());
            if (!lastMonth.equals(YearMonth.now())) {
                account.setCurrentMonthMovements(0);
            }
        }
    }

    private Mono<Void> validateFixedTermTransaction(Account account) {
        if (account.getWithdrawalDay() == null) {
            return Mono.error(new BusinessException(
                "Fixed-term accounts require a configured movement day",
                "MISSING_FIXED_TERM_MOVEMENT_DAY"));
        }

        int configuredDay = account.getWithdrawalDay().getDayOfMonth();
        if (LocalDateTime.now().getDayOfMonth() != configuredDay) {
            return Mono.error(new BusinessException(
                "Fixed-term account movements only allowed on day " + configuredDay + " of each month",
                "INVALID_FIXED_TERM_MOVEMENT_DATE"));
        }

        YearMonth currentMonth = YearMonth.now();
        LocalDateTime monthStart = currentMonth.atDay(1).atStartOfDay();
        LocalDateTime monthEnd = currentMonth.atEndOfMonth().atTime(LocalTime.MAX);

        return transactionRepository.countByProductIdAndDateBetween(account.getId(), monthStart, monthEnd)
            .flatMap(count -> count > 0
                ? Mono.error(new BusinessException(
                    "Fixed-term accounts only allow one deposit or withdrawal on the configured day each month",
                    "MAX_FIXED_TERM_MOVEMENTS_EXCEEDED"))
                : Mono.empty());
    }

    private Mono<Void> validateTransactionAgainstCredit(Transaction transaction, Credit credit) {
        String transactionType = transaction.getTransactionType().toUpperCase();
        String creditType = credit.getCreditType().toUpperCase();

        if ("CREDIT_CARD".equals(creditType)) {
            if (!("PAYMENT".equals(transactionType) || "CHARGE".equals(transactionType))) {
                return Mono.error(new BusinessException(
                    "Credit cards only support PAYMENT and CHARGE transactions",
                    "INVALID_CREDIT_CARD_TRANSACTION"));
            }

            if ("CHARGE".equals(transactionType)) {
                BigDecimal available = credit.getCreditLimit().subtract(credit.getOutstandingBalance());
                if (available.compareTo(transaction.getAmount()) < 0) {
                    return Mono.error(new BusinessException(
                        "Credit card limit exceeded",
                        "CREDIT_LIMIT_EXCEEDED"));
                }
            }

            if ("PAYMENT".equals(transactionType)
                && credit.getOutstandingBalance().compareTo(transaction.getAmount()) < 0) {
                return Mono.error(new BusinessException(
                    "Payment exceeds outstanding balance",
                    "PAYMENT_EXCEEDS_OUTSTANDING_BALANCE"));
            }

            return Mono.empty();
        }

        if (!"PAYMENT".equals(transactionType)) {
            return Mono.error(new BusinessException(
                "Credits only support PAYMENT transactions",
                "INVALID_CREDIT_TRANSACTION"));
        }

        if (credit.getOutstandingBalance().compareTo(transaction.getAmount()) < 0) {
            return Mono.error(new BusinessException(
                "Payment exceeds outstanding balance",
                "PAYMENT_EXCEEDS_OUTSTANDING_BALANCE"));
        }

        return Mono.empty();
    }

    private Mono<Void> applyTransactionFeePreview(Transaction transaction, Account account) {
        return calculateAppliedFee(account)
            .flatMap(appliedFee -> {
                transaction.setAppliedFee(appliedFee);

                String transactionType = transaction.getTransactionType().toUpperCase();
                if ("DEPOSIT".equals(transactionType) && transaction.getAmount().compareTo(appliedFee) <= 0) {
                    return Mono.error(new BusinessException(
                        "Deposit amount must be greater than the applied transaction fee",
                        "INVALID_DEPOSIT_AMOUNT_FOR_FEE"));
                }

                if ("WITHDRAWAL".equals(transactionType)) {
                    BigDecimal totalDebit = transaction.getAmount().add(appliedFee);
                    if (account.getBalance().compareTo(totalDebit) < 0) {
                        return Mono.error(new BusinessException(
                            "Insufficient funds available including transaction fee",
                            "INSUFFICIENT_BALANCE"));
                    }
                }

                return Mono.empty();
            });
    }

    private Mono<Void> verifyTransferOwnership(String clientId, Account sourceAccount) {
        boolean isOwner = clientId.equals(sourceAccount.getClientId())
            || (sourceAccount.getHolders() != null && sourceAccount.getHolders().contains(clientId));

        if (!isOwner) {
            return Mono.error(new BusinessException(
                "Source account does not belong to the specified client",
                "ACCOUNT_NOT_OWNED_BY_CLIENT"));
        }

        return Mono.empty();
    }

    private Mono<Void> verifyDebitCardOwnership(String clientId, DebitCard debitCard) {
        if (!clientId.equals(debitCard.getClientId())) {
            return Mono.error(new BusinessException(
                "Debit card does not belong to the specified client",
                "DEBIT_CARD_NOT_OWNED_BY_CLIENT"));
        }
        return Mono.empty();
    }

    private Mono<BigDecimal> calculateAppliedFee(Account account) {
        if (account.getTransactionFee() == null || account.getTransactionFee().compareTo(ZERO) <= 0) {
            return Mono.just(ZERO);
        }

        if (account.getFreeTransactions() == null || account.getFreeTransactions() < 0) {
            return Mono.just(ZERO);
        }

        YearMonth currentMonth = YearMonth.now();
        LocalDateTime monthStart = currentMonth.atDay(1).atStartOfDay();
        LocalDateTime monthEnd = currentMonth.atEndOfMonth().atTime(LocalTime.MAX);

        return transactionRepository.countByProductIdAndDateBetween(account.getId(), monthStart, monthEnd)
            .map(transactionCount -> transactionCount >= account.getFreeTransactions()
                ? account.getTransactionFee()
                : ZERO);
    }

    private Mono<Void> applyTransaction(Transaction transaction) {
        if ("ACCOUNT".equalsIgnoreCase(transaction.getProductType())) {
            return updateAccountBalance(transaction.getProductId(), transaction);
        }
        if ("CREDIT".equalsIgnoreCase(transaction.getProductType())) {
            return updateCreditBalance(transaction.getProductId(), transaction);
        }
        return Mono.error(new BusinessException("Invalid product type: " + transaction.getProductType(), "INVALID_PRODUCT_TYPE"));
    }

    private Mono<Void> verifyClientOwnsProduct(String clientId, String productType, String productId) {
        if ("ACCOUNT".equalsIgnoreCase(productType)) {
            return accountRepository.findById(productId)
                .switchIfEmpty(Mono.error(new BusinessException("Account not found", "ACCOUNT_NOT_FOUND")))
                .flatMap(account -> {
                    boolean isOwner = clientId.equals(account.getClientId())
                        || (account.getHolders() != null && account.getHolders().contains(clientId));
                    if (!isOwner) {
                        return Mono.error(new BusinessException(
                            "Account does not belong to the specified client",
                            "ACCOUNT_NOT_OWNED_BY_CLIENT"));
                    }
                    return Mono.empty();
                });
        }
        if ("CREDIT".equalsIgnoreCase(productType)) {
            return creditRepository.findById(productId)
                .switchIfEmpty(Mono.error(new BusinessException("Credit not found", "CREDIT_NOT_FOUND")))
                .flatMap(credit -> {
                    if (!clientId.equals(credit.getClientId())) {
                        return Mono.error(new BusinessException(
                            "Credit does not belong to the specified client",
                            "CREDIT_NOT_OWNED_BY_CLIENT"));
                    }
                    return Mono.empty();
                });
        }

        return Mono.error(new BusinessException("Invalid product type: " + productType, "INVALID_PRODUCT_TYPE"));
    }

    private Mono<Void> updateAccountBalance(String accountId, Transaction transaction) {
        return accountRepository.findById(accountId)
            .switchIfEmpty(Mono.error(new BusinessException("Account not found", "ACCOUNT_NOT_FOUND")))
            .flatMap(account -> {
                String transactionType = transaction.getTransactionType().toUpperCase();
                BigDecimal appliedFee = transaction.getAppliedFee() == null ? ZERO : transaction.getAppliedFee();
                if ("DEPOSIT".equals(transactionType)) {
                    account.setBalance(account.getBalance().add(transaction.getAmount()).subtract(appliedFee));
                    if ("SAVINGS".equalsIgnoreCase(account.getAccountType())) {
                        resetSavingsCounterIfNeeded(account);
                        account.setCurrentMonthMovements(account.getCurrentMonthMovements() + 1);
                    }
                } else if ("WITHDRAWAL".equals(transactionType)) {
                    account.setBalance(account.getBalance().subtract(transaction.getAmount()).subtract(appliedFee));
                    if ("SAVINGS".equalsIgnoreCase(account.getAccountType())) {
                        resetSavingsCounterIfNeeded(account);
                        account.setCurrentMonthMovements(account.getCurrentMonthMovements() + 1);
                    }
                }
                account.setLastMovementDate(LocalDateTime.now());
                return accountRepository.save(account).then();
            });
    }

    private Mono<Void> updateCreditBalance(String creditId, Transaction transaction) {
        return creditRepository.findById(creditId)
            .switchIfEmpty(Mono.error(new BusinessException("Credit not found", "CREDIT_NOT_FOUND")))
            .flatMap(credit -> {
                String transactionType = transaction.getTransactionType().toUpperCase();
                if ("PAYMENT".equals(transactionType)) {
                    credit.setOutstandingBalance(credit.getOutstandingBalance().subtract(transaction.getAmount()));
                } else if ("CHARGE".equals(transactionType)) {
                    credit.setOutstandingBalance(credit.getOutstandingBalance().add(transaction.getAmount()));
                }
                return creditRepository.save(credit).then();
            });
    }

    private Mono<TransferResponse> executeTransfer(TransferRequest request, Account source, Account destination) {
        String reference = "TRF-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();

        Transaction debitTransaction = new Transaction();
        debitTransaction.setProductId(source.getId());
        debitTransaction.setProductType("ACCOUNT");
        debitTransaction.setTransactionType("WITHDRAWAL");
        debitTransaction.setAmount(request.getAmount());
        debitTransaction.setDescription(buildTransferDescription(
            "Transfer to account " + destination.getId(),
            request.getDescription()));
        debitTransaction.setReference(reference);

        Transaction creditTransaction = new Transaction();
        creditTransaction.setProductId(destination.getId());
        creditTransaction.setProductType("ACCOUNT");
        creditTransaction.setTransactionType("DEPOSIT");
        creditTransaction.setAmount(request.getAmount());
        creditTransaction.setDescription(buildTransferDescription(
            "Transfer from account " + source.getId(),
            request.getDescription()));
        creditTransaction.setReference(reference);

        return persistOwnedTransaction(debitTransaction)
            .flatMap(savedDebit -> persistOwnedTransaction(creditTransaction)
                .map(savedCredit -> new TransferResponse(reference, savedDebit, savedCredit)));
    }

    private Mono<ThirdPartyCreditPaymentResponse> executeThirdPartyCreditPayment(
            ThirdPartyCreditPaymentRequest request,
            Account payerAccount,
            Credit credit) {
        String reference = "TPP-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();

        Transaction withdrawalTransaction = new Transaction();
        withdrawalTransaction.setProductId(payerAccount.getId());
        withdrawalTransaction.setProductType("ACCOUNT");
        withdrawalTransaction.setTransactionType("WITHDRAWAL");
        withdrawalTransaction.setAmount(request.getAmount());
        withdrawalTransaction.setDescription(buildTransferDescription(
            "Third-party credit payment to credit " + credit.getId(),
            request.getDescription()));
        withdrawalTransaction.setReference(reference);

        Transaction creditPaymentTransaction = new Transaction();
        creditPaymentTransaction.setProductId(credit.getId());
        creditPaymentTransaction.setProductType("CREDIT");
        creditPaymentTransaction.setTransactionType("PAYMENT");
        creditPaymentTransaction.setAmount(request.getAmount());
        creditPaymentTransaction.setDescription(buildTransferDescription(
            "Payment received from third-party account " + payerAccount.getId(),
            request.getDescription()));
        creditPaymentTransaction.setReference(reference);

        return persistOwnedTransaction(withdrawalTransaction)
            .flatMap(savedWithdrawal -> persistOwnedTransaction(creditPaymentTransaction)
                .map(savedCreditPayment -> new ThirdPartyCreditPaymentResponse(
                    reference,
                    savedWithdrawal,
                    savedCreditPayment
                )));
    }

    private Mono<Transaction> executeDebitCardPayment(DebitCardPaymentRequest request, DebitCard debitCard) {
        Transaction paymentTransaction = new Transaction();
        paymentTransaction.setProductId(debitCard.getPrimaryAccountId());
        paymentTransaction.setProductType("ACCOUNT");
        paymentTransaction.setTransactionType("WITHDRAWAL");
        paymentTransaction.setAmount(request.getAmount());
        paymentTransaction.setDescription(buildTransferDescription(
            "Debit card payment using card " + debitCard.getCardNumber(),
            request.getDescription()));
        paymentTransaction.setReference("DCP-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase());

        return persistOwnedTransaction(paymentTransaction);
    }

    public Mono<Transaction> persistOwnedTransaction(Transaction transaction) {
        return validateTransaction(transaction)
            .map(valid -> {
                valid.setDate(LocalDateTime.now());
                return valid;
            })
            .flatMap(transactionRepository::save)
            .flatMap(saved -> applyTransaction(saved).thenReturn(saved));
    }

    private String buildTransferDescription(String baseDescription, String customDescription) {
        if (customDescription == null || customDescription.trim().isEmpty()) {
            return baseDescription;
        }
        return baseDescription + " - " + customDescription.trim();
    }
}
