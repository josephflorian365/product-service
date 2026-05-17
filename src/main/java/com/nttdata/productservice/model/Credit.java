package com.nttdata.productservice.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.math.BigDecimal;
import java.time.LocalDate;

/**
 * Entity representing a credit product.
 */
@Document(collection = "credits")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Credit {

    @Id
    private String id;

    private String clientId;

    private String creditType; // PERSONAL, BUSINESS, CREDIT_CARD

    private BigDecimal amount;

    private BigDecimal interestRate;

    private Integer termMonths;

    private BigDecimal outstandingBalance;

    private BigDecimal creditLimit; // for credit cards

    private LocalDate dueDate;
}
