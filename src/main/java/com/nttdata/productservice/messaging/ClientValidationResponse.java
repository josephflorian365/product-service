package com.nttdata.productservice.messaging;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ClientValidationResponse {

    private String requestId;

    private boolean found;

    private String clientId;

    private String clientType;

    private String profile;

    private String errorCode;

    private String errorMessage;
}
