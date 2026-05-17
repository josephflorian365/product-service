package com.nttdata.productservice.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class YankiPaymentResponse {

    private String reference;

    private Transaction sourceTransaction;

    private Transaction destinationTransaction;
}
