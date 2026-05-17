package com.nttdata.productservice.controller;

import com.nttdata.productservice.model.Account;
import com.nttdata.productservice.model.ProductBalance;
import com.nttdata.productservice.service.AccountService;
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
@RequestMapping("/accounts")
@RequiredArgsConstructor
@Tag(name = "Account API", description = "API for managing bank accounts")
public class AccountController {

    private final AccountService accountService;

    @PostMapping
    @Operation(summary = "Create a new account")
    public Mono<ResponseEntity<Account>> createAccount(@RequestBody Account account) {
        return Mono.fromCompletionStage(accountService.createAccount(account).toCompletionStage())
            .map(createdAccount -> ResponseEntity.status(HttpStatus.CREATED).body(createdAccount));
    }

    @GetMapping
    @Operation(summary = "Get all accounts")
    public Flux<Account> getAllAccounts() {
        return Flux.from(accountService.getAllAccounts());
    }

    @GetMapping("/{id}")
    @Operation(summary = "Get account by ID")
    public Mono<ResponseEntity<Account>> getAccountById(@PathVariable String id) {
        return Mono.from(accountService.getAccountById(id).toFlowable())
            .map(ResponseEntity::ok)
            .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    @PutMapping("/{id}")
    @Operation(summary = "Update an account")
    public Mono<ResponseEntity<Account>> updateAccount(@PathVariable String id, @RequestBody Account account) {
        return Mono.fromCompletionStage(accountService.updateAccount(id, account).toCompletionStage()).map(ResponseEntity::ok);
    }

    @DeleteMapping("/{id}")
    @Operation(summary = "Delete an account")
    public Mono<ResponseEntity<Void>> deleteAccount(@PathVariable String id) {
        return Mono.from(accountService.deleteAccount(id).toFlowable())
            .thenReturn(ResponseEntity.<Void>noContent().build());
    }

    @GetMapping("/client/{clientId}")
    @Operation(summary = "Get accounts by client ID")
    public Flux<Account> getAccountsByClientId(@PathVariable String clientId) {
        return Flux.from(accountService.getAccountsByClientId(clientId));
    }

    @GetMapping("/client/{clientId}/{accountId}")
    @Operation(summary = "Get account by client ID and account ID")
    public Mono<ResponseEntity<Account>> getAccountByClientAndId(
            @PathVariable String clientId,
            @PathVariable String accountId) {
        return Mono.from(accountService.getAccountByClientAndId(clientId, accountId).toFlowable())
            .map(ResponseEntity::ok)
            .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    @GetMapping("/client/{clientId}/balances")
    @Operation(summary = "Get account balances by client ID")
    public Flux<ProductBalance> getAccountBalancesByClientId(@PathVariable String clientId) {
        return Flux.from(accountService.getAccountBalancesByClientId(clientId));
    }
}
