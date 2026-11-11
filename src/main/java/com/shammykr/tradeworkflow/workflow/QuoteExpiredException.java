package com.shammykr.tradeworkflow.workflow;

import java.util.UUID;

public class QuoteExpiredException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public QuoteExpiredException(UUID rfqId, UUID quoteId) {
    super("quote " + quoteId + " on RFQ " + rfqId + " has expired");
  }
}
