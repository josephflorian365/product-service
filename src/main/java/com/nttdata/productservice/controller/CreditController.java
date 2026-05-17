package com.nttdata.productservice.controller;

import com.nttdata.productservice.model.Credit;
import com.nttdata.productservice.model.ProductBalance;
import com.nttdata.productservice.service.CreditService;
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
@RequestMapping("/credits")
@RequiredArgsConstructor
@Tag(name = "Credit API", description = "API for managing credits")
public class CreditController {

    private final CreditService creditService;

    @PostMapping
    @Operation(summary = "Create a new credit")
    public Mono<ResponseEntity<Credit>> createCredit(@RequestBody Credit credit) {
        return Mono.fromCompletionStage(creditService.createCredit(credit).toCompletionStage())
            .map(createdCredit -> ResponseEntity.status(HttpStatus.CREATED).body(createdCredit));
    }

    @GetMapping
    @Operation(summary = "Get all credits")
    public Flux<Credit> getAllCredits() {
        return Flux.from(creditService.getAllCredits());
    }

    @GetMapping("/{id}")
    @Operation(summary = "Get credit by ID")
    public Mono<ResponseEntity<Credit>> getCreditById(@PathVariable String id) {
        return Mono.from(creditService.getCreditById(id).toFlowable())
            .map(ResponseEntity::ok)
            .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    @PutMapping("/{id}")
    @Operation(summary = "Update a credit")
    public Mono<ResponseEntity<Credit>> updateCredit(@PathVariable String id, @RequestBody Credit credit) {
        return Mono.fromCompletionStage(creditService.updateCredit(id, credit).toCompletionStage()).map(ResponseEntity::ok);
    }

    @DeleteMapping("/{id}")
    @Operation(summary = "Delete a credit")
    public Mono<ResponseEntity<Void>> deleteCredit(@PathVariable String id) {
        return Mono.from(creditService.deleteCredit(id).toFlowable())
            .thenReturn(ResponseEntity.<Void>noContent().build());
    }

    @GetMapping("/client/{clientId}")
    @Operation(summary = "Get credits by client ID")
    public Flux<Credit> getCreditsByClientId(@PathVariable String clientId) {
        return Flux.from(creditService.getCreditsByClientId(clientId));
    }

    @GetMapping("/client/{clientId}/{creditId}")
    @Operation(summary = "Get credit by client ID and credit ID")
    public Mono<ResponseEntity<Credit>> getCreditByClientAndId(
            @PathVariable String clientId,
            @PathVariable String creditId) {
        return Mono.from(creditService.getCreditByClientAndId(clientId, creditId).toFlowable())
            .map(ResponseEntity::ok)
            .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    @GetMapping("/client/{clientId}/balances")
    @Operation(summary = "Get credit balances by client ID")
    public Flux<ProductBalance> getCreditBalancesByClientId(@PathVariable String clientId) {
        return Flux.from(creditService.getCreditBalancesByClientId(clientId));
    }
}
