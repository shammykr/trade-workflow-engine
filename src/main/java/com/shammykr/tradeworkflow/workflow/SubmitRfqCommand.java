package com.shammykr.tradeworkflow.workflow;

import com.shammykr.tradeworkflow.domain.ProductType;
import java.math.BigDecimal;

public record SubmitRfqCommand(
    String clientId,
    String underlier,
    ProductType productType,
    BigDecimal notional,
    String currency,
    int maturityMonths) {}
