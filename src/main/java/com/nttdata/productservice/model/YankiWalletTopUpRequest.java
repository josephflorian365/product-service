package com.nttdata.productservice.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class YankiWalletTopUpRequest {

    private BigDecimal amount;

    private String description;
}
