package com.shammykr.tradeworkflow.domain;

import java.util.UUID;

/** A requested state change is not allowed from the RFQ's current state. */
public class InvalidTransitionException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public InvalidTransitionException(UUID rfqId, RfqStatus from, RfqStatus to) {
    super("RFQ " + rfqId + " cannot move from " + from + " to " + to);
  }

  public InvalidTransitionException(String message) {
    super(message);
  }
}
