package com.shammykr.tradeworkflow.stub;

import java.math.BigDecimal;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Behaviour of the simulated external systems.
 *
 * @param pricing latency range and the notional above which the desk refuses to price
 * @param booking latency, and the client-id prefix the booking system rejects (failed KYC)
 */
@ConfigurationProperties("stubs")
public record StubProperties(boolean enabled, Pricing pricing, Booking booking) {

  public record Pricing(Duration minLatency, Duration maxLatency, BigDecimal maxNotional) {}

  public record Booking(Duration latency, String blockedClientPrefix) {}
}
