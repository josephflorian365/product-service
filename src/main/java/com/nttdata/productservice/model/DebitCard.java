package com.nttdata.productservice.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.LocalDateTime;

/**
 * Debit card linked to a primary bank account.
 */
@Document(collection = "debit_cards")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DebitCard {

    @Id
    private String id;

    private String clientId;

    private String primaryAccountId;

    private String cardNumber;

    private LocalDateTime createdDate;
}
