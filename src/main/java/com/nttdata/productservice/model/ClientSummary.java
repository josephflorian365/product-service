package com.nttdata.productservice.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Lightweight client view retrieved from client-service for business validations.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ClientSummary {

    private String id;

    private String clientType;

    private String profile;
}
