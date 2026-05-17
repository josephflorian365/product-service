package com.nttdata.productservice.messaging;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ClientValidationRequest {

    private String requestId;

    private String clientId;
}
