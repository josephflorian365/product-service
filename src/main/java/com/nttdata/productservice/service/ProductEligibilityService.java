package com.nttdata.productservice.service;

import com.nttdata.productservice.exception.BusinessException;
import com.nttdata.productservice.repository.CreditRepositoryReactive;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.LocalDate;

/**
 * Validates whether a client can acquire new banking products.
 */
@Service
@RequiredArgsConstructor
public class ProductEligibilityService {

    private static final BigDecimal ZERO = BigDecimal.ZERO;

    private final CreditRepositoryReactive creditRepository;

    public Mono<Void> validateNoOverdueDebt(String clientId) {
        return creditRepository.existsByClientIdAndOutstandingBalanceGreaterThanAndDueDateBefore(
                clientId,
                ZERO,
                LocalDate.now())
            .flatMap(hasOverdueDebt -> hasOverdueDebt
                ? Mono.error(new BusinessException(
                    "Client cannot acquire new products while having overdue credit debt",
                    "CLIENT_HAS_OVERDUE_DEBT"))
                : Mono.empty());
    }
}
