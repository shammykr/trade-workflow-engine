package com.shammykr.tradeworkflow.integration;

import com.shammykr.tradeworkflow.config.WorkflowProperties;
import com.shammykr.tradeworkflow.messaging.PricingRequest;
import com.shammykr.tradeworkflow.workflow.PricingGateway;
import java.net.http.HttpClient;
import org.springframework.core.env.Environment;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

/**
 * Calls the pricing service's REST API: {@code POST /stub/pricing/requests} returns 202 Accepted;
 * the price is published later on {@code pricing-responses}.
 */
@Component
public class RestPricingClient implements PricingGateway {

  private final RestClient http;
  private final String configuredBaseUrl;
  private final Environment env;

  public RestPricingClient(WorkflowProperties props, Environment env) {
    HttpClient client = HttpClient.newBuilder().connectTimeout(props.pricing().timeout()).build();
    JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(client);
    factory.setReadTimeout(props.pricing().timeout());
    this.http = RestClient.builder().requestFactory(factory).build();
    this.configuredBaseUrl = props.pricing().baseUrl();
    this.env = env;
  }

  @Override
  public void requestPrice(PricingRequest request) {
    try {
      http.post()
          .uri(baseUrl() + "/stub/pricing/requests")
          .body(request)
          .retrieve()
          .toBodilessEntity();
    } catch (RestClientException e) {
      throw new PricingUnavailableException("pricing service call failed: " + e.getMessage(), e);
    }
  }

  /** An empty base URL means "this application's own stub", on whatever port it bound. */
  private String baseUrl() {
    if (configuredBaseUrl != null && !configuredBaseUrl.isBlank()) {
      return configuredBaseUrl;
    }
    return "http://localhost:" + env.getProperty("local.server.port", "8080");
  }
}
