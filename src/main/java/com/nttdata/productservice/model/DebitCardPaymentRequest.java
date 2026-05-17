package com.nttdata.productservice.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * Request to pay using a debit card against its linked primary account.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DebitCardPaymentRequest {

    private String clientId;

    private String debitCardId;

    private BigDecimal amount;

    private String description;
}
