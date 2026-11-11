package com.shammykr.tradeworkflow.messaging;

import com.shammykr.tradeworkflow.domain.ProductType;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/** Message on {@code booking-events}: a request to book an accepted quote. */
public record BookingEvent(
    UUID eventId,
    Type type,
    UUID rfqId,
    UUID quoteId,
    String clientId,
    String underlier,
    ProductType productType,
    BigDecimal notional,
    String currency,
    int maturityMonths,
    BigDecimal pricePct,
    Instant requestedAt) {

  public enum Type {
    BOOKING_REQUESTED
  }
}
