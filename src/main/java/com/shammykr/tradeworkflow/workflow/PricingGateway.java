package com.shammykr.tradeworkflow.workflow;

import com.shammykr.tradeworkflow.messaging.PricingRequest;

/** Outbound port to the pricing service. The price itself arrives later on Kafka. */
public interface PricingGateway {

  /**
   * Asks for a price. Throws {@link PricingUnavailableException} if the service can't be reached.
   */
  void requestPrice(PricingRequest request);

  class PricingUnavailableException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public PricingUnavailableException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
