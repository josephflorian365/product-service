package com.nttdata.productservice.service;

import com.nttdata.productservice.exception.BusinessException;
import com.nttdata.productservice.model.DebitCard;
import com.nttdata.productservice.model.ProductReportResponse;
import com.nttdata.productservice.model.Transaction;
import com.nttdata.productservice.repository.DebitCardRepositoryReactive;
import com.nttdata.productservice.repository.TransactionRepositoryReactive;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ReportServiceTest {

    @Mock
    private DebitCardRepositoryReactive debitCardRepository;

    @Mock
    private TransactionRepositoryReactive transactionRepository;

    @InjectMocks
    private ReportService reportService;

    @Test
    void getProductReportAggregatesTransactionsByProduct() {
        LocalDateTime start = LocalDateTime.of(2026, 5, 1, 0, 0);
        LocalDateTime end = LocalDateTime.of(2026, 5, 31, 23, 59);

        when(transactionRepository.findByProductTypeAndDateBetweenOrderByDateDesc(eq("ACCOUNT"), eq(start), eq(end)))
            .thenReturn(Flux.just(
                transaction("acc-1", "ACCOUNT", "DEPOSIT", "100.00", "1.50"),
                transaction("acc-1", "ACCOUNT", "WITHDRAWAL", "30.00", "0.50"),
                transaction("acc-2", "ACCOUNT", "DEPOSIT", "80.00", "0.00")
            ));

        ProductReportResponse report = reportService.getProductReport("ACCOUNT", start, end).block();

        assertEquals(2, report.getItems().size());
        assertEquals("acc-1", report.getItems().get(0).getProductId());
        assertEquals(new BigDecimal("130.00"), report.getItems().get(0).getTotalAmount());
        assertEquals(new BigDecimal("2.00"), report.getItems().get(0).getTotalFees());
    }

    @Test
    void getLastTenDebitCardMovementsFailsWhenCardDoesNotExist() {
        when(debitCardRepository.findById("missing-card")).thenReturn(Mono.empty());

        BusinessException exception = assertThrows(
            BusinessException.class,
            () -> reportService.getLastTenDebitCardMovements("missing-card").firstOrError().blockingGet()
        );

        assertEquals("DEBIT_CARD_NOT_FOUND", exception.getErrorCode());
    }

    @Test
    void getLastTenDebitCardMovementsUsesPrimaryAccountTransactions() {
        when(debitCardRepository.findById("card-1"))
            .thenReturn(Mono.just(new DebitCard("card-1", "cli-1", "acc-1", "4111111111111111", LocalDateTime.now())));
        when(transactionRepository.findTop10ByProductIdOrderByDateDesc(eq("acc-1")))
            .thenReturn(Flux.just(transaction("acc-1", "ACCOUNT", "WITHDRAWAL", "25.00", "0.00")));

        Transaction transaction = reportService.getLastTenDebitCardMovements("card-1").firstOrError().blockingGet();

        assertEquals("acc-1", transaction.getProductId());
    }

    private Transaction transaction(
            String productId,
            String productType,
            String transactionType,
            String amount,
            String appliedFee) {
        Transaction transaction = new Transaction();
        transaction.setProductId(productId);
        transaction.setProductType(productType);
        transaction.setTransactionType(transactionType);
        transaction.setAmount(new BigDecimal(amount));
        transaction.setAppliedFee(new BigDecimal(appliedFee));
        transaction.setDate(LocalDateTime.now());
        return transaction;
    }
}
