package com.shammykr.tradeworkflow.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @param quoteTtl how long a quote stays firm
 * @param pricing where the pricing service lives; an empty base URL means this application's own
 *     stub pricing endpoint
 */
@ConfigurationProperties("workflow")
public record WorkflowProperties(Duration quoteTtl, Duration expiryCheckInterval, Pricing pricing) {

  public record Pricing(String baseUrl, Duration timeout) {}
}
