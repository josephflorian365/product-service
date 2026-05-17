package com.nttdata.productservice.controller;

import com.nttdata.productservice.model.ProductReportResponse;
import com.nttdata.productservice.model.Transaction;
import com.nttdata.productservice.service.ReportService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

@RestController
@RequestMapping("/reports")
@RequiredArgsConstructor
@Tag(name = "Report API", description = "API for product reports")
public class ReportController {

    private final ReportService reportService;

    @GetMapping("/products/{productType}")
    @Operation(summary = "Get product report by type and date interval")
    public Mono<ProductReportResponse> getProductReport(
            @PathVariable String productType,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime startDate,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime endDate) {
        return reportService.getProductReport(productType, startDate, endDate);
    }

    @GetMapping("/credits/{creditId}/last-10-movements")
    @Operation(summary = "Get last 10 movements for a credit or credit card")
    public Flux<Transaction> getLastTenCreditMovements(@PathVariable String creditId) {
        return Flux.from(reportService.getLastTenCreditMovements(creditId));
    }

    @GetMapping("/debit-cards/{debitCardId}/last-10-movements")
    @Operation(summary = "Get last 10 movements for a debit card")
    public Flux<Transaction> getLastTenDebitCardMovements(@PathVariable String debitCardId) {
        return Flux.from(reportService.getLastTenDebitCardMovements(debitCardId));
    }
}
