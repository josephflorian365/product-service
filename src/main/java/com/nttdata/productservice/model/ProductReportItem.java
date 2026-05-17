package com.nttdata.productservice.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * Aggregated transaction report for a single product in a time interval.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ProductReportItem {

    private String productId;

    private String productType;

    private long totalMovements;

    private BigDecimal totalAmount;

    private BigDecimal totalFees;
}
