package com.nttdata.productservice.service;

import com.nttdata.productservice.exception.BusinessException;
import com.nttdata.productservice.repository.DebitCardRepositoryReactive;
import com.nttdata.productservice.model.ProductReportItem;
import com.nttdata.productservice.model.ProductReportResponse;
import com.nttdata.productservice.model.Transaction;
import com.nttdata.productservice.repository.TransactionRepositoryReactive;
import io.reactivex.rxjava3.core.Flowable;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class ReportService {

    private static final BigDecimal ZERO = BigDecimal.ZERO;

    private final DebitCardRepositoryReactive debitCardRepository;
    private final TransactionRepositoryReactive transactionRepository;

    public Mono<ProductReportResponse> getProductReport(
            String productType,
            LocalDateTime startDate,
            LocalDateTime endDate) {
        validateInterval(startDate, endDate);
        validateProductType(productType);

        return transactionRepository
            .findByProductTypeAndDateBetweenOrderByDateDesc(productType.toUpperCase(), startDate, endDate)
            .collectList()
            .map(transactions -> new ProductReportResponse(
                productType.toUpperCase(),
                startDate,
                endDate,
                aggregateTransactions(productType, transactions)
            ));
    }

    public Flowable<Transaction> getLastTenCreditMovements(String creditId) {
        return Flowable.fromPublisher(
            transactionRepository.findTop10ByProductIdOrderByDateDesc(creditId)
        );
    }

    public Flowable<Transaction> getLastTenDebitCardMovements(String debitCardId) {
        return Flowable.fromPublisher(
            debitCardRepository.findById(debitCardId)
                .switchIfEmpty(Mono.error(new BusinessException("Debit card not found", "DEBIT_CARD_NOT_FOUND")))
                .flatMapMany(debitCard ->
                    transactionRepository.findTop10ByProductIdOrderByDateDesc(debitCard.getPrimaryAccountId()))
        );
    }

    private void validateInterval(LocalDateTime startDate, LocalDateTime endDate) {
        if (startDate == null || endDate == null) {
            throw new BusinessException("Start date and end date are required", "INVALID_REPORT_INTERVAL");
        }
        if (startDate.isAfter(endDate)) {
            throw new BusinessException("Start date must be before end date", "INVALID_REPORT_INTERVAL");
        }
    }

    private void validateProductType(String productType) {
        if (!("ACCOUNT".equalsIgnoreCase(productType) || "CREDIT".equalsIgnoreCase(productType))) {
            throw new BusinessException("Product type must be ACCOUNT or CREDIT", "INVALID_PRODUCT_TYPE");
        }
    }

    private List<ProductReportItem> aggregateTransactions(String productType, List<Transaction> transactions) {
        Map<String, List<Transaction>> byProduct = transactions.stream()
            .collect(Collectors.groupingBy(Transaction::getProductId));

        return byProduct.entrySet().stream()
            .map(entry -> {
                BigDecimal totalAmount = entry.getValue().stream()
                    .map(Transaction::getAmount)
                    .reduce(ZERO, BigDecimal::add);

                BigDecimal totalFees = entry.getValue().stream()
                    .map(transaction -> transaction.getAppliedFee() == null ? ZERO : transaction.getAppliedFee())
                    .reduce(ZERO, BigDecimal::add);

                return new ProductReportItem(
                    entry.getKey(),
                    productType.toUpperCase(),
                    entry.getValue().size(),
                    totalAmount,
                    totalFees
                );
            })
            .sorted(Comparator.comparing(ProductReportItem::getProductId))
            .collect(Collectors.toList());
    }
}
