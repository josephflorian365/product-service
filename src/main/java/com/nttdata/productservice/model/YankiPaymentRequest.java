package com.nttdata.productservice.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class YankiPaymentRequest {

    private String sourcePhoneNumber;

    private String destinationPhoneNumber;

    private BigDecimal amount;

    private String description;
}
