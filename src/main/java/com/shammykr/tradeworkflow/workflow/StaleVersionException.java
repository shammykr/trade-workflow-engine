package com.shammykr.tradeworkflow.workflow;

import java.util.UUID;

/** The client acted on an old version of the RFQ (If-Match did not match). */
public class StaleVersionException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  private final long currentVersion;

  public StaleVersionException(UUID id, long expected, long current) {
    super("RFQ " + id + " is at version " + current + ", request was based on version " + expected);
    this.currentVersion = current;
  }

  public long getCurrentVersion() {
    return currentVersion;
  }
}
