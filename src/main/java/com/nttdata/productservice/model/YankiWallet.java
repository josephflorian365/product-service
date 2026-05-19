package com.nttdata.productservice.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Document(collection = "yanki_wallets")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class YankiWallet {

    @Id
    private String id;

    private String documentType;

    private String documentNumber;

    private String phoneNumber;

    private String imei;

    private String email;

    private String linkedDebitCardId;

    private BigDecimal balance;

    private LocalDateTime createdDate;
}
