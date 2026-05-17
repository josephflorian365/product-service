package com.nttdata.productservice.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Transfer execution result.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TransferResponse {

    private String reference;

    private Transaction debitTransaction;

    private Transaction creditTransaction;
}
