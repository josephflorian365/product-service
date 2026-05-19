package com.nttdata.productservice.controller;

import com.nttdata.productservice.model.YankiDebitCardLinkRequest;
import com.nttdata.productservice.model.YankiPaymentRequest;
import com.nttdata.productservice.model.YankiPaymentResponse;
import com.nttdata.productservice.model.YankiWallet;
import com.nttdata.productservice.model.YankiWalletTopUpRequest;
import com.nttdata.productservice.service.YankiWalletService;
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
@RequestMapping("/yanki-wallets")
@RequiredArgsConstructor
@Tag(name = "Yanki API", description = "API for Yanki mobile wallets")
public class YankiWalletController {

    private final YankiWalletService yankiWalletService;

    @PostMapping
    @Operation(summary = "Create a Yanki wallet")
    public Mono<ResponseEntity<YankiWallet>> createWallet(@RequestBody YankiWallet wallet) {
        return Mono.fromCompletionStage(yankiWalletService.createWallet(wallet).toCompletionStage())
            .map(created -> ResponseEntity.status(HttpStatus.CREATED).body(created));
    }

    @GetMapping
    @Operation(summary = "Get all Yanki wallets")
    public Flux<YankiWallet> getAllWallets() {
        return Flux.from(yankiWalletService.getAllWallets());
    }

    @GetMapping("/{id}")
    @Operation(summary = "Get Yanki wallet by ID")
    public Mono<ResponseEntity<YankiWallet>> getWalletById(@PathVariable String id) {
        return Mono.from(yankiWalletService.getWalletById(id).toFlowable())
            .map(ResponseEntity::ok)
            .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    @PutMapping("/{id}")
    @Operation(summary = "Update a Yanki wallet")
    public Mono<ResponseEntity<YankiWallet>> updateWallet(@PathVariable String id, @RequestBody YankiWallet wallet) {
        return Mono.fromCompletionStage(yankiWalletService.updateWallet(id, wallet).toCompletionStage())
            .map(ResponseEntity::ok);
    }

    @PutMapping("/{walletId}/debit-card")
    @Operation(summary = "Link a Yanki wallet to a debit card")
    public Mono<ResponseEntity<YankiWallet>> linkDebitCard(
            @PathVariable String walletId,
            @RequestBody YankiDebitCardLinkRequest request) {
        return Mono.fromCompletionStage(yankiWalletService.linkDebitCard(walletId, request).toCompletionStage())
            .map(ResponseEntity::ok);
    }

    @PostMapping("/payments")
    @Operation(summary = "Send a payment between Yanki wallets using phone numbers")
    public Mono<ResponseEntity<YankiPaymentResponse>> sendPayment(@RequestBody YankiPaymentRequest request) {
        return Mono.fromCompletionStage(yankiWalletService.sendPayment(request).toCompletionStage())
            .map(response -> ResponseEntity.status(HttpStatus.CREATED).body(response));
    }

    @PostMapping("/{walletId}/top-ups")
    @Operation(summary = "Top up a Yanki wallet balance")
    public Mono<ResponseEntity<YankiWallet>> topUpWallet(
            @PathVariable String walletId,
            @RequestBody YankiWalletTopUpRequest request) {
        return Mono.fromCompletionStage(yankiWalletService.topUpWallet(walletId, request).toCompletionStage())
            .map(updated -> ResponseEntity.status(HttpStatus.CREATED).body(updated));
    }

    @DeleteMapping("/{id}")
    @Operation(summary = "Delete a Yanki wallet")
    public Mono<ResponseEntity<Void>> deleteWallet(@PathVariable String id) {
        return Mono.fromCompletionStage(yankiWalletService.deleteWallet(id).toCompletionStage())
            .thenReturn(ResponseEntity.<Void>noContent().build());
    }
}
