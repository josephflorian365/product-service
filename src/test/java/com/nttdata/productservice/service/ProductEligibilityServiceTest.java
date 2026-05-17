package com.nttdata.productservice.service;

import com.nttdata.productservice.exception.BusinessException;
import com.nttdata.productservice.repository.CreditRepositoryReactive;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ProductEligibilityServiceTest {

    @Mock
    private CreditRepositoryReactive creditRepository;

    @InjectMocks
    private ProductEligibilityService productEligibilityService;

    @Test
    void validateNoOverdueDebtFailsWhenClientHasExpiredDebt() {
        when(creditRepository.existsByClientIdAndOutstandingBalanceGreaterThanAndDueDateBefore(
            org.mockito.ArgumentMatchers.eq("cli-1"),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any())
        ).thenReturn(Mono.just(true));

        BusinessException exception = assertThrows(
            BusinessException.class,
            () -> productEligibilityService.validateNoOverdueDebt("cli-1").block()
        );

        assertEquals("CLIENT_HAS_OVERDUE_DEBT", exception.getErrorCode());
    }

    @Test
    void validateNoOverdueDebtCompletesWhenDebtIsCurrent() {
        when(creditRepository.existsByClientIdAndOutstandingBalanceGreaterThanAndDueDateBefore(
            org.mockito.ArgumentMatchers.eq("cli-1"),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any())
        ).thenReturn(Mono.just(false));

        assertDoesNotThrow(() -> productEligibilityService.validateNoOverdueDebt("cli-1").block());
    }
}
