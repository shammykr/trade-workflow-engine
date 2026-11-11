package com.shammykr.tradeworkflow.stub;

import com.shammykr.tradeworkflow.domain.ProductType;
import com.shammykr.tradeworkflow.messaging.EventPublisher;
import com.shammykr.tradeworkflow.messaging.PricingRequest;
import com.shammykr.tradeworkflow.messaging.PricingResponse;
import com.shammykr.tradeworkflow.messaging.Topics;
import jakarta.annotation.PreDestroy;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/**
 * Simulated pricing service. Accepts a pricing request over REST (202), then after a random latency
 * publishes the price on {@code pricing-responses}, as an asynchronous quant pricing engine would.
 *
 * <p>The price is a toy model in percent of notional: a base level per product, adjusted for
 * maturity, plus a deterministic per-underlier skew. Requests above {@code max-notional} are
 * refused (desk limit).
 */
@RestController
@RequestMapping("/stub/pricing")
@ConditionalOnProperty(prefix = "stubs", name = "enabled", havingValue = "true")
public class StubPricingService {

  private static final Logger log = LoggerFactory.getLogger(StubPricingService.class);

  private static final Map<ProductType, Double> BASE_PCT =
      Map.of(
          ProductType.AUTOCALLABLE, 98.40,
          ProductType.REVERSE_CONVERTIBLE, 97.10,
          ProductType.CAPITAL_PROTECTED_NOTE, 100.60,
          ProductType.BARRIER_REVERSE_CONVERTIBLE, 96.30);

  private final EventPublisher events;
  private final StubProperties.Pricing props;
  private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);

  public StubPricingService(EventPublisher events, StubProperties props) {
    this.events = events;
    this.props = props.pricing();
  }

  @PostMapping("/requests")
  @ResponseStatus(HttpStatus.ACCEPTED)
  public Map<String, String> request(@RequestBody PricingRequest request) {
    long min = props.minLatency().toMillis();
    long max = Math.max(min, props.maxLatency().toMillis());
    long delay = ThreadLocalRandom.current().nextLong(min, max + 1);
    executor.schedule(() -> respond(request), delay, TimeUnit.MILLISECONDS);
    return Map.of("pricingRequestId", request.pricingRequestId(), "status", "ACCEPTED");
  }

  private void respond(PricingRequest req) {
    PricingResponse response;
    if (req.notional().compareTo(props.maxNotional()) > 0) {
      response =
          new PricingResponse(
              UUID.randomUUID(),
              req.pricingRequestId(),
              req.rfqId(),
              PricingResponse.Status.REFUSED,
              null,
              "notional "
                  + req.notional().toPlainString()
                  + " exceeds desk limit "
                  + props.maxNotional().toPlainString(),
              Instant.now());
    } else {
      response =
          new PricingResponse(
              UUID.randomUUID(),
              req.pricingRequestId(),
              req.rfqId(),
              PricingResponse.Status.PRICED,
              price(req),
              null,
              Instant.now());
    }
    log.atInfo()
        .setMessage("stub pricing responded")
        .addKeyValue("rfqId", req.rfqId())
        .addKeyValue("status", response.status())
        .addKeyValue("pricePct", response.pricePct())
        .log();
    events.publish(Topics.PRICING_RESPONSES, req.rfqId(), response);
  }

  static BigDecimal price(PricingRequest req) {
    double pct = BASE_PCT.get(req.productType());
    pct -= 0.04 * req.maturityMonths(); // longer-dated: more embedded optionality sold
    pct += (Math.floorMod(req.underlier().hashCode(), 200) - 100) / 100.0; // skew in [-1, +1)
    return BigDecimal.valueOf(pct).setScale(4, RoundingMode.HALF_UP);
  }

  @PreDestroy
  void shutdown() {
    executor.shutdownNow();
  }
}
