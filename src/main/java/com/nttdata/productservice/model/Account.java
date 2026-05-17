package com.nttdata.productservice.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Entity representing a bank account.
 * Supports three account types: SAVINGS, CURRENT (checking), and FIXED_TERM (time deposits).
 * 
 * Account Type Details:
 * - SAVINGS: No maintenance fee, limited monthly movements
 * - CURRENT: Has maintenance fee, unlimited movements
 * - FIXED_TERM: Free, single withdrawal/deposit on specific day
 */
@Document(collection = "accounts")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Account {

    @Id
    private String id;

    /**
     * Primary owner reference for compatibility with existing flows.
     */
    private String clientId;

    /**
     * Account holders. Business accounts may contain one or more holders.
     */
    private List<String> holders;

    /**
     * Authorized signers for business accounts. May be empty.
     */
    private List<String> authorizedSigners;
    
    /**
     * Type of client: PERSONAL or EMPRESARIAL.
     * Used for business rule validations.
     */
    private String clientType; // PERSONAL, EMPRESARIAL

    private String accountType; // SAVINGS, CURRENT, FIXED_TERM

    private BigDecimal balance;

    private BigDecimal minimumOpeningAmount;

    /**
     * Required minimum daily average balance for profile-specific savings products.
     */
    private BigDecimal minimumDailyAverage;

    private Integer maxMonthlyMovements; // for savings accounts

    private BigDecimal maintenanceFee; // for current accounts

    private LocalDate withdrawalDay; // for fixed term accounts (specific day of month)

    private Integer freeTransactions;

    private BigDecimal transactionFee;
    
    /**
     * Account creation date for auditing and business rule validation.
     */
    private LocalDateTime createdDate;
    
    /**
     * Count of movements in current month for SAVINGS accounts.
     */
    private Integer currentMonthMovements = 0;
    
    /**
     * Last movement date for resetting monthly counter.
     */
    private LocalDateTime lastMovementDate;
}
