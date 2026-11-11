package com.shammykr.tradeworkflow.messaging;

import com.shammykr.tradeworkflow.domain.ProductType;
import java.math.BigDecimal;
import java.util.UUID;

/** Body of {@code POST /stub/pricing/requests}: what the workflow asks the pricing service. */
public record PricingRequest(
    String pricingRequestId,
    UUID rfqId,
    String underlier,
    ProductType productType,
    BigDecimal notional,
    String currency,
    int maturityMonths) {}
