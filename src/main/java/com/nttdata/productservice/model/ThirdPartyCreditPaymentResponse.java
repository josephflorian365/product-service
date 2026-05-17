package com.nttdata.productservice.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Result of paying a third-party credit product.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ThirdPartyCreditPaymentResponse {

    private String reference;

    private Transaction accountWithdrawal;

    private Transaction creditPayment;
}
