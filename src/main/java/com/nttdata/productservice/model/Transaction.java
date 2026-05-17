package com.nttdata.productservice.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Entity representing a transaction on a product.
 */
@Document(collection = "transactions")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Transaction {

    @Id
    private String id;

    private String productId;

    private String productType; // ACCOUNT, CREDIT

    private String transactionType; // DEPOSIT, WITHDRAWAL, PAYMENT, CHARGE

    private BigDecimal amount;

    /**
     * Fee applied to the transaction when free transaction limits are exceeded.
     */
    private BigDecimal appliedFee;

    private LocalDateTime date;

    private String description;

    private String reference;
}
