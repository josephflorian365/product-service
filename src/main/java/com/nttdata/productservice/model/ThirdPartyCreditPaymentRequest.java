package com.nttdata.productservice.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * Request to pay a third-party credit product from a bank account.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ThirdPartyCreditPaymentRequest {

    private String payerClientId;

    private String payerAccountId;

    private String creditId;

    private BigDecimal amount;

    private String description;
}
