package com.shammykr.tradeworkflow.api;

import com.shammykr.tradeworkflow.domain.ProductType;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import java.math.BigDecimal;

public record SubmitRfqRequest(
    @NotBlank @Size(max = 64) String clientId,
    @NotBlank @Size(max = 32) String underlier,
    @NotNull ProductType productType,
    @NotNull @DecimalMin(value = "1000", message = "must be at least 1000") BigDecimal notional,
    @NotNull @Pattern(regexp = "[A-Z]{3}", message = "must be an ISO currency code")
        String currency,
    @Min(1) @Max(120) int maturityMonths) {}
