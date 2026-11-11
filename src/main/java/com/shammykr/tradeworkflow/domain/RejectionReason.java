package com.shammykr.tradeworkflow.domain;

/** Why an RFQ ended in {@link RfqStatus#REJECTED}. */
public enum RejectionReason {
  PRICING_REFUSED,
  PRICING_UNAVAILABLE,
  CLIENT_DECLINED,
  QUOTE_EXPIRED,
  BOOKING_REJECTED
}
