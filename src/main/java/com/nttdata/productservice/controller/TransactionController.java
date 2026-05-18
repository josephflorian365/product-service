package com.nttdata.productservice.controller;

import com.nttdata.productservice.model.Transaction;
import com.nttdata.productservice.model.ThirdPartyCreditPaymentRequest;
import com.nttdata.productservice.model.ThirdPartyCreditPaymentResponse;
import com.nttdata.productservice.model.DebitCardPaymentRequest;
import com.nttdata.productservice.model.TransferRequest;
import com.nttdata.productservice.model.TransferResponse;
import com.nttdata.productservice.service.TransactionService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/transactions")
@RequiredArgsConstructor
@Tag(name = "Transaction API", description = "API for managing transactions")
public class TransactionController {

    private final TransactionService transactionService;

    @PostMapping
    @Operation(summary = "Create a new transaction")
    public Mono<ResponseEntity<Transaction>> createTransaction(@RequestBody Transaction transaction) {
        return Mono.fromCompletionStage(transactionService.createTransaction(transaction).toCompletionStage())
            .map(createdTransaction -> ResponseEntity.status(HttpStatus.CREATED).body(createdTransaction));
    }

    @PostMapping("/transfers")
    @Operation(summary = "Create a transfer between bank accounts")
    public Mono<ResponseEntity<TransferResponse>> createTransfer(@RequestBody TransferRequest transferRequest) {
        return Mono.fromCompletionStage(transactionService.createTransfer(transferRequest).toCompletionStage())
            .map(response -> ResponseEntity.status(HttpStatus.CREATED).body(response));
    }

    @PostMapping("/third-party-credit-payments")
    @Operation(summary = "Pay a third-party credit product from a bank account")
    public Mono<ResponseEntity<ThirdPartyCreditPaymentResponse>> createThirdPartyCreditPayment(
            @RequestBody ThirdPartyCreditPaymentRequest paymentRequest) {
        return Mono.fromCompletionStage(
                transactionService.createThirdPartyCreditPayment(paymentRequest).toCompletionStage())
            .map(response -> ResponseEntity.status(HttpStatus.CREATED).body(response));
    }

    @PostMapping("/debit-card-payments")
    @Operation(summary = "Pay using a debit card")
    public Mono<ResponseEntity<Transaction>> createDebitCardPayment(
            @RequestBody DebitCardPaymentRequest paymentRequest) {
        return Mono.fromCompletionStage(transactionService.createDebitCardPayment(paymentRequest).toCompletionStage())
            .map(response -> ResponseEntity.status(HttpStatus.CREATED).body(response));
    }

    @GetMapping
    @Operation(summary = "Get all transactions")
    public Flux<Transaction> getAllTransactions() {
        return Flux.from(transactionService.getAllTransactions());
    }

    @GetMapping("/{id}")
    @Operation(summary = "Get transaction by ID")
    public Mono<ResponseEntity<Transaction>> getTransactionById(@PathVariable String id) {
        return Mono.from(transactionService.getTransactionById(id).toFlowable())
            .map(ResponseEntity::ok)
            .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    @PutMapping("/{id}")
    @Operation(summary = "Update a transaction")
    public Mono<ResponseEntity<Transaction>> updateTransaction(
            @PathVariable String id,
            @RequestBody Transaction transaction) {
        return Mono.fromCompletionStage(
                transactionService.updateTransaction(id, transaction).toCompletionStage())
            .map(ResponseEntity::ok);
    }

    @DeleteMapping("/{id}")
    @Operation(summary = "Delete a transaction")
    public Mono<ResponseEntity<Void>> deleteTransaction(@PathVariable String id) {
        return Mono.from(transactionService.deleteTransaction(id).toFlowable())
            .thenReturn(ResponseEntity.<Void>noContent().build());
    }

    @GetMapping("/product/{productId}")
    @Operation(summary = "Get transactions by product ID")
    public Flux<Transaction> getTransactionsByProductId(@PathVariable String productId) {
        return Flux.from(transactionService.getTransactionsByProductId(productId));
    }

    @GetMapping("/client/{clientId}/product/{productType}/{productId}")
    @Operation(summary = "Get transactions by client ID and owned product")
    public Flux<Transaction> getTransactionsByClientAndProductId(
            @PathVariable String clientId,
            @PathVariable String productType,
            @PathVariable String productId) {
        return Flux.from(
            transactionService.getTransactionsByClientAndProductId(clientId, productType, productId)
        );
    }
}
