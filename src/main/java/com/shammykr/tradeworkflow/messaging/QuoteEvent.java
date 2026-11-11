package com.shammykr.tradeworkflow.messaging;

import com.shammykr.tradeworkflow.domain.ProductType;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/** Message on {@code quote-events}. */
public record QuoteEvent(
    UUID eventId,
    Type type,
    UUID rfqId,
    UUID quoteId,
    String clientId,
    String underlier,
    ProductType productType,
    BigDecimal notional,
    String currency,
    BigDecimal pricePct,
    Instant validUntil,
    Instant occurredAt) {

  public enum Type {
    QUOTE_CREATED,
    QUOTE_ACCEPTED,
    QUOTE_DECLINED,
    QUOTE_EXPIRED
  }
}
