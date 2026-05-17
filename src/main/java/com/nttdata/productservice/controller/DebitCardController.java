package com.nttdata.productservice.controller;

import com.nttdata.productservice.model.DebitCard;
import com.nttdata.productservice.service.DebitCardService;
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
@RequestMapping("/debit-cards")
@RequiredArgsConstructor
@Tag(name = "Debit Card API", description = "API for managing debit cards")
public class DebitCardController {

    private final DebitCardService debitCardService;

    @PostMapping
    @Operation(summary = "Create a new debit card")
    public Mono<ResponseEntity<DebitCard>> createDebitCard(@RequestBody DebitCard debitCard) {
        return Mono.fromCompletionStage(debitCardService.createDebitCard(debitCard).toCompletionStage())
            .map(created -> ResponseEntity.status(HttpStatus.CREATED).body(created));
    }

    @GetMapping
    @Operation(summary = "Get all debit cards")
    public Flux<DebitCard> getAllDebitCards() {
        return Flux.from(debitCardService.getAllDebitCards());
    }

    @GetMapping("/client/{clientId}")
    @Operation(summary = "Get debit cards by client ID")
    public Flux<DebitCard> getDebitCardsByClientId(@PathVariable String clientId) {
        return Flux.from(debitCardService.getDebitCardsByClientId(clientId));
    }

    @GetMapping("/{id}")
    @Operation(summary = "Get debit card by ID")
    public Mono<ResponseEntity<DebitCard>> getDebitCardById(@PathVariable String id) {
        return Mono.from(debitCardService.getDebitCardById(id).toFlowable())
            .map(ResponseEntity::ok)
            .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    @PutMapping("/{id}")
    @Operation(summary = "Update a debit card")
    public Mono<ResponseEntity<DebitCard>> updateDebitCard(@PathVariable String id, @RequestBody DebitCard debitCard) {
        return Mono.fromCompletionStage(debitCardService.updateDebitCard(id, debitCard).toCompletionStage())
            .map(ResponseEntity::ok);
    }

    @DeleteMapping("/{id}")
    @Operation(summary = "Delete a debit card")
    public Mono<ResponseEntity<Void>> deleteDebitCard(@PathVariable String id) {
        return Mono.fromCompletionStage(debitCardService.deleteDebitCard(id).toCompletionStage())
            .thenReturn(ResponseEntity.<Void>noContent().build());
    }
}
