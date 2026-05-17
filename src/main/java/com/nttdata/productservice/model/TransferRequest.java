package com.nttdata.productservice.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * Transfer request between bank accounts in the same institution.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TransferRequest {

    private String sourceClientId;

    private String sourceAccountId;

    private String destinationAccountId;

    private BigDecimal amount;

    private String description;
}
