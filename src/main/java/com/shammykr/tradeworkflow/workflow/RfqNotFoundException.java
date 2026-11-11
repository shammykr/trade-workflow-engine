package com.shammykr.tradeworkflow.workflow;

import java.util.UUID;

public class RfqNotFoundException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public RfqNotFoundException(UUID id) {
    super("RFQ " + id + " not found");
  }
}
