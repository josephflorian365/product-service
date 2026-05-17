package com.nttdata.productservice.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * Balance view for account and credit products.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ProductBalance {

    private String productId;

    private String productType;

    private String subType;

    private BigDecimal currentBalance;

    private BigDecimal availableBalance;
}
