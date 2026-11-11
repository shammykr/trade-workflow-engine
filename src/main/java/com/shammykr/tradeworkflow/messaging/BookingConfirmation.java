package com.shammykr.tradeworkflow.messaging;

import java.time.Instant;
import java.util.UUID;

/** Message on {@code booking-confirmations}: the booking system's answer. */
public record BookingConfirmation(
    UUID eventId,
    UUID rfqId,
    UUID quoteId,
    Status status,
    String tradeId,
    String reason,
    Instant occurredAt) {

  public enum Status {
    CONFIRMED,
    REJECTED
  }
}
