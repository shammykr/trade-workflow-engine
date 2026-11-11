package com.shammykr.tradeworkflow.messaging;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/** Message on {@code pricing-responses}. */
public record PricingResponse(
    UUID eventId,
    String pricingRequestId,
    UUID rfqId,
    Status status,
    BigDecimal pricePct,
    String reason,
    Instant pricedAt) {

  public enum Status {
    PRICED,
    REFUSED
  }
}
