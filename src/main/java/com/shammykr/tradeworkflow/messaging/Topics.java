package com.shammykr.tradeworkflow.messaging;

/** Kafka topic names. Every message is keyed by RFQ id so events for one RFQ stay ordered. */
public final class Topics {

  /** Pricing service → workflow: price (or refusal) for a pricing request. */
  public static final String PRICING_RESPONSES = "pricing-responses";

  /** Workflow → anyone interested (sales UI, client notifications): quote lifecycle. */
  public static final String QUOTE_EVENTS = "quote-events";

  /** Workflow → booking system: book an accepted quote. */
  public static final String BOOKING_EVENTS = "booking-events";

  /** Booking system → workflow: booking confirmed or rejected. */
  public static final String BOOKING_CONFIRMATIONS = "booking-confirmations";

  private Topics() {}
}
