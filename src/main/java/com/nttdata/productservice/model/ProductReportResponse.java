package com.nttdata.productservice.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Product report response for an interval.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ProductReportResponse {

    private String productType;

    private LocalDateTime startDate;

    private LocalDateTime endDate;

    private List<ProductReportItem> items;
}
