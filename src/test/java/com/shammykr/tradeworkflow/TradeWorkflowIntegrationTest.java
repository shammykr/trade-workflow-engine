package com.shammykr.tradeworkflow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shammykr.tradeworkflow.api.RfqResponse;
import com.shammykr.tradeworkflow.domain.RejectionReason;
import com.shammykr.tradeworkflow.domain.RfqStatus;
import com.shammykr.tradeworkflow.messaging.PricingResponse;
import com.shammykr.tradeworkflow.messaging.Topics;
import io.micrometer.core.instrument.MeterRegistry;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.redpanda.RedpandaContainer;

/**
 * End-to-end workflow scenarios against real PostgreSQL and Kafka (Redpanda) containers.
 *
 * <p>The application runs on a random port with the stub pricing service and stub booking system
 * enabled, so every scenario goes through the real integration paths: REST call to the pricing
 * service, price back on {@code pricing-responses}, quote notification on {@code quote-events},
 * booking request on {@code booking-events}, answer on {@code booking-confirmations}. An
 * independent Kafka consumer records every message so the tests can assert on the Kafka flow, not
 * only on the database state.
 */
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
      "workflow.quote-ttl=3s",
      "workflow.expiry-check-interval=200ms",
      "stubs.pricing.min-latency=10ms",
      "stubs.pricing.max-latency=50ms",
      "stubs.booking.latency=50ms",
    })
@Testcontainers
@AutoConfigureObservability
class TradeWorkflowIntegrationTest {

  @Container @ServiceConnection
  static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16");

  @Container @ServiceConnection
  static RedpandaContainer kafka = new RedpandaContainer("redpandadata/redpanda:v26.2.6");

  private static final Duration TIMEOUT = Duration.ofSeconds(15);
  private static final List<ConsumerRecord<String, String>> observed = new CopyOnWriteArrayList<>();
  private static final AtomicBoolean observing = new AtomicBoolean(true);
  private static Thread observer;

  @Autowired TestRestTemplate http;
  @Autowired ObjectMapper json;
  @Autowired KafkaTemplate<String, String> kafkaTemplate;
  @Autowired MeterRegistry meters;

  /** Records every message on the workflow topics, like an external audit consumer would. */
  @BeforeAll
  static void startObserver() {
    Properties p = new Properties();
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    p.put(ConsumerConfig.GROUP_ID_CONFIG, "it-observer");
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    p.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, 500);
    observer =
        Thread.ofPlatform()
            .daemon()
            .start(
                () -> {
                  try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p)) {
                    consumer.subscribe(
                        java.util.regex.Pattern.compile(
                            "pricing-responses|quote-events|booking-events|booking-confirmations"));
                    while (observing.get()) {
                      consumer.poll(Duration.ofMillis(100)).forEach(observed::add);
                    }
                  }
                });
  }

  @AfterAll
  static void stopObserver() throws InterruptedException {
    observing.set(false);
    observer.join(5000);
  }

  // ---- scenarios -----------------------------------------------------------------------------

  @Test
  @DisplayName("happy path: PENDING -> QUOTED -> ACCEPTED -> BOOKED with the full Kafka flow")
  void happyPath() {
    ResponseEntity<RfqResponse> submitted = submit("CLIENT-ACME", "SX5E", 5_000_000);
    assertThat(submitted.getStatusCode()).isEqualTo(HttpStatus.CREATED);
    assertThat(submitted.getBody().status()).isEqualTo(RfqStatus.PENDING);
    UUID id = submitted.getBody().id();

    RfqResponse quoted = awaitStatus(id, RfqStatus.QUOTED);
    assertThat(quoted.quotes()).hasSize(1);
    RfqResponse.QuoteView quote = quoted.quotes().getFirst();
    assertThat(quote.pricePct()).isPositive();
    assertThat(Duration.between(quote.createdAt(), quote.validUntil()))
        .isEqualTo(Duration.ofSeconds(3));
    awaitEvent(Topics.PRICING_RESPONSES, id, "status", "PRICED");
    JsonNode created = awaitEvent(Topics.QUOTE_EVENTS, id, "type", "QUOTE_CREATED");
    assertThat(created.get("quoteId").asText()).isEqualTo(quote.id().toString());

    // Accept with the ETag from the last read, as a desk UI would.
    ResponseEntity<RfqResponse> accepted =
        action(id, "accept", quote.id(), etagOf(id), "trader.jane");
    assertThat(accepted.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(accepted.getBody().status()).isEqualTo(RfqStatus.ACCEPTED);

    JsonNode booking = awaitEvent(Topics.BOOKING_EVENTS, id, "type", "BOOKING_REQUESTED");
    assertThat(booking.get("quoteId").asText()).isEqualTo(quote.id().toString());
    assertThat(new BigDecimal(booking.get("pricePct").asText()))
        .isEqualByComparingTo(quote.pricePct());
    awaitEvent(Topics.QUOTE_EVENTS, id, "type", "QUOTE_ACCEPTED");

    RfqResponse booked = awaitStatus(id, RfqStatus.BOOKED);
    JsonNode confirmation = awaitEvent(Topics.BOOKING_CONFIRMATIONS, id, "status", "CONFIRMED");
    assertThat(booked.tradeId()).startsWith("TRD-").isEqualTo(confirmation.get("tradeId").asText());

    assertThat(transitions(id))
        .containsExactly(
            "null->PENDING", "PENDING->QUOTED", "QUOTED->ACCEPTED", "ACCEPTED->BOOKED");
    assertThat(actors(id))
        .containsExactly("api", "pricing-service", "trader.jane", "booking-system");
    assertThat(meters.get("rfq.time.to.book").timer().count()).isPositive();
  }

  @Test
  @DisplayName("pricing refused: notional above the desk limit -> REJECTED(PRICING_REFUSED)")
  void pricingRefused() {
    UUID id = submit("CLIENT-ACME", "SPX", 75_000_000).getBody().id();

    RfqResponse rejected = awaitStatus(id, RfqStatus.REJECTED);
    assertThat(rejected.rejectionReason()).isEqualTo(RejectionReason.PRICING_REFUSED);
    assertThat(rejected.rejectionDetail()).contains("exceeds desk limit");
    assertThat(rejected.quotes()).isEmpty();
    awaitEvent(Topics.PRICING_RESPONSES, id, "status", "REFUSED");
    assertThat(eventsFor(Topics.QUOTE_EVENTS, id)).isEmpty();
    assertThat(transitions(id)).containsExactly("null->PENDING", "PENDING->REJECTED");
  }

  @Test
  @DisplayName("booking rejected: blocked client -> ACCEPTED -> REJECTED(BOOKING_REJECTED)")
  void bookingRejected() {
    UUID id = submit("BLOCKED-SHELLCO", "NKY", 2_000_000).getBody().id();
    UUID quoteId = awaitStatus(id, RfqStatus.QUOTED).quotes().getFirst().id();

    assertThat(action(id, "accept", quoteId, null, "trader.jane").getStatusCode())
        .isEqualTo(HttpStatus.OK);

    RfqResponse rejected = awaitStatus(id, RfqStatus.REJECTED);
    assertThat(rejected.rejectionReason()).isEqualTo(RejectionReason.BOOKING_REJECTED);
    assertThat(rejected.rejectionDetail()).contains("KYC");
    assertThat(rejected.tradeId()).isNull();
    awaitEvent(Topics.BOOKING_CONFIRMATIONS, id, "status", "REJECTED");
    assertThat(transitions(id))
        .containsExactly(
            "null->PENDING", "PENDING->QUOTED", "QUOTED->ACCEPTED", "ACCEPTED->REJECTED");
  }

  @Test
  @DisplayName(
      "quote expiry: no answer within the TTL -> REJECTED(QUOTE_EXPIRED); late accept is 409")
  void quoteExpires() {
    UUID id = submit("CLIENT-SLOWPOKE", "AAPL", 1_000_000).getBody().id();
    UUID quoteId = awaitStatus(id, RfqStatus.QUOTED).quotes().getFirst().id();

    RfqResponse expired = awaitStatus(id, RfqStatus.REJECTED);
    assertThat(expired.rejectionReason()).isEqualTo(RejectionReason.QUOTE_EXPIRED);
    assertThat(expired.quotes().getFirst().status().name()).isEqualTo("EXPIRED");
    awaitEvent(Topics.QUOTE_EVENTS, id, "type", "QUOTE_EXPIRED");

    ResponseEntity<String> late = rawAction(id, "accept", quoteId, null);
    assertThat(late.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
    assertThat(eventsFor(Topics.BOOKING_EVENTS, id)).isEmpty();
  }

  @Test
  @DisplayName("client declines: QUOTED -> REJECTED(CLIENT_DECLINED), nothing sent to booking")
  void clientDeclines() {
    UUID id = submit("CLIENT-ACME", "MSFT", 3_000_000).getBody().id();
    UUID quoteId = awaitStatus(id, RfqStatus.QUOTED).quotes().getFirst().id();

    ResponseEntity<RfqResponse> declined = action(id, "decline", quoteId, null, "sales.bob");
    assertThat(declined.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(declined.getBody().status()).isEqualTo(RfqStatus.REJECTED);
    assertThat(declined.getBody().rejectionReason()).isEqualTo(RejectionReason.CLIENT_DECLINED);
    awaitEvent(Topics.QUOTE_EVENTS, id, "type", "QUOTE_DECLINED");
    assertThat(eventsFor(Topics.BOOKING_EVENTS, id)).isEmpty();
  }

  @Test
  @DisplayName("concurrent desk activity: 8 simultaneous accepts -> exactly one wins, one booking")
  void concurrentAcceptsExactlyOneWins() throws Exception {
    UUID id = submit("CLIENT-ACME", "TSLA", 4_000_000).getBody().id();
    UUID quoteId = awaitStatus(id, RfqStatus.QUOTED).quotes().getFirst().id();

    int traders = 8;
    CountDownLatch start = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(traders);
    List<Future<ResponseEntity<String>>> results = new ArrayList<>();
    for (int i = 0; i < traders; i++) {
      results.add(
          pool.submit(
              () -> {
                start.await();
                return rawAction(id, "accept", quoteId, null);
              }));
    }
    start.countDown();
    List<HttpStatus> statuses = new ArrayList<>();
    int lockConflicts = 0;
    for (Future<ResponseEntity<String>> f : results) {
      ResponseEntity<String> r = f.get();
      statuses.add(HttpStatus.valueOf(r.getStatusCode().value()));
      if (r.getBody() != null && r.getBody().contains("Concurrent update")) {
        lockConflicts++;
      }
    }
    pool.shutdown();
    // Losers either collided with the winner's write (JPA @Version -> "Concurrent update") or
    // read the RFQ after the winner committed (state check -> "Invalid state transition").
    System.out.printf(
        "concurrent accepts: %d optimistic-lock conflicts, %d state-check conflicts%n",
        lockConflicts, traders - 1 - lockConflicts);

    assertThat(statuses).filteredOn(s -> s == HttpStatus.OK).hasSize(1);
    assertThat(statuses).filteredOn(s -> s == HttpStatus.CONFLICT).hasSize(traders - 1);
    assertThat(lockConflicts).as("losers rejected by JPA optimistic locking").isPositive();
    awaitStatus(id, RfqStatus.BOOKED);
    assertThat(eventsFor(Topics.BOOKING_EVENTS, id)).hasSize(1);
    assertThat(transitions(id)).containsOnlyOnce("QUOTED->ACCEPTED");
  }

  @Test
  @DisplayName(
      "optimistic locking over HTTP: If-Match with a stale version -> 412, state unchanged")
  void staleIfMatchIsRejected() {
    UUID id = submit("CLIENT-ACME", "GOOG", 1_500_000).getBody().id();
    long pendingVersion = Long.parseLong(etagOf(id));
    UUID quoteId = awaitStatus(id, RfqStatus.QUOTED).quotes().getFirst().id();

    ResponseEntity<String> stale = rawAction(id, "accept", quoteId, Long.toString(pendingVersion));
    assertThat(stale.getStatusCode()).isEqualTo(HttpStatus.PRECONDITION_FAILED);
    assertThat(stale.getBody()).contains("currentVersion");
    assertThat(get(id).status()).isEqualTo(RfqStatus.QUOTED);

    // With the current version the same action succeeds.
    assertThat(action(id, "accept", quoteId, etagOf(id), "trader.jane").getStatusCode())
        .isEqualTo(HttpStatus.OK);
  }

  @Test
  @DisplayName("idempotent consumer: a duplicate pricing response does not create a second quote")
  void duplicatePricingResponseIsIgnored() throws Exception {
    UUID id = submit("CLIENT-ACME", "AMZN", 2_500_000).getBody().id();
    RfqResponse quoted = awaitStatus(id, RfqStatus.QUOTED);
    JsonNode original = awaitEvent(Topics.PRICING_RESPONSES, id, "status", "PRICED");
    double ignoredBefore = ignoredCount();

    PricingResponse duplicate = json.treeToValue(original, PricingResponse.class);
    kafkaTemplate
        .send(Topics.PRICING_RESPONSES, id.toString(), json.writeValueAsString(duplicate))
        .get();

    await().atMost(TIMEOUT).until(() -> ignoredCount() > ignoredBefore);
    RfqResponse after = get(id);
    assertThat(after.status()).isEqualTo(RfqStatus.QUOTED);
    assertThat(after.quotes()).hasSize(1);
    assertThat(after.version()).isEqualTo(quoted.version());
  }

  @Test
  @DisplayName("API errors: invalid transition 409, unknown RFQ 404, invalid request 400")
  void apiErrors() {
    UUID refused = submit("CLIENT-ACME", "SPX", 90_000_000).getBody().id();
    awaitStatus(refused, RfqStatus.REJECTED);
    assertThat(rawAction(refused, "accept", UUID.randomUUID(), null).getStatusCode())
        .isEqualTo(HttpStatus.CONFLICT);

    assertThat(http.getForEntity("/api/rfqs/" + UUID.randomUUID(), String.class).getStatusCode())
        .isEqualTo(HttpStatus.NOT_FOUND);

    Map<String, Object> invalid =
        Map.of(
            "clientId",
            "",
            "underlier",
            "SPX",
            "productType",
            "AUTOCALLABLE",
            "notional",
            10,
            "currency",
            "usd",
            "maturityMonths",
            0);
    assertThat(http.postForEntity("/api/rfqs", invalid, String.class).getStatusCode())
        .isEqualTo(HttpStatus.BAD_REQUEST);
  }

  @Test
  @DisplayName("observability: health is UP with db and kafka; workflow metrics are exported")
  void observability() throws Exception {
    UUID id = submit("CLIENT-ACME", "SX5E", 1_000_000).getBody().id();
    awaitStatus(id, RfqStatus.QUOTED);

    JsonNode health = json.readTree(http.getForObject("/actuator/health", String.class));
    assertThat(health.get("status").asText()).isEqualTo("UP");
    assertThat(health.at("/components/db/status").asText()).isEqualTo("UP");
    assertThat(health.at("/components/kafka/status").asText()).isEqualTo("UP");

    String prometheus = http.getForObject("/actuator/prometheus", String.class);
    assertThat(prometheus)
        .contains("rfq_submitted_total", "rfq_transitions_total", "rfq_status_count");
  }

  // ---- helpers -------------------------------------------------------------------------------

  private ResponseEntity<RfqResponse> submit(String client, String underlier, long notional) {
    Map<String, Object> body =
        Map.of(
            "clientId",
            client,
            "underlier",
            underlier,
            "productType",
            "AUTOCALLABLE",
            "notional",
            notional,
            "currency",
            "USD",
            "maturityMonths",
            24);
    return http.postForEntity("/api/rfqs", body, RfqResponse.class);
  }

  private RfqResponse get(UUID id) {
    return http.getForObject("/api/rfqs/" + id, RfqResponse.class);
  }

  private String etagOf(UUID id) {
    return http.getForEntity("/api/rfqs/" + id, RfqResponse.class)
        .getHeaders()
        .getETag()
        .replace("\"", "");
  }

  private RfqResponse awaitStatus(UUID id, RfqStatus status) {
    return await()
        .atMost(TIMEOUT)
        .pollInterval(Duration.ofMillis(50))
        .until(() -> get(id), r -> r.status() == status);
  }

  private ResponseEntity<RfqResponse> action(
      UUID id, String verb, UUID quoteId, String ifMatch, String actor) {
    return http.exchange(
        "/api/rfqs/" + id + "/" + verb,
        HttpMethod.POST,
        entity(quoteId, ifMatch, actor),
        RfqResponse.class);
  }

  private ResponseEntity<String> rawAction(UUID id, String verb, UUID quoteId, String ifMatch) {
    return http.exchange(
        "/api/rfqs/" + id + "/" + verb,
        HttpMethod.POST,
        entity(quoteId, ifMatch, "trader"),
        String.class);
  }

  private static HttpEntity<Map<String, Object>> entity(
      UUID quoteId, String ifMatch, String actor) {
    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);
    headers.set("X-Desk-User", actor);
    if (ifMatch != null) {
      headers.setIfMatch("\"" + ifMatch + "\"");
    }
    return new HttpEntity<>(Map.of("quoteId", quoteId.toString()), headers);
  }

  private List<String> transitions(UUID id) {
    return transitionViews(id).stream().map(t -> t.from() + "->" + t.to()).toList();
  }

  private List<String> actors(UUID id) {
    return transitionViews(id).stream().map(RfqResponse.TransitionView::actor).toList();
  }

  private List<RfqResponse.TransitionView> transitionViews(UUID id) {
    return List.of(
        http.getForObject("/api/rfqs/" + id + "/transitions", RfqResponse.TransitionView[].class));
  }

  private List<JsonNode> eventsFor(String topic, UUID rfqId) {
    List<JsonNode> out = new ArrayList<>();
    for (ConsumerRecord<String, String> r : observed) {
      if (r.topic().equals(topic) && rfqId.toString().equals(r.key())) {
        try {
          out.add(json.readTree(r.value()));
        } catch (Exception e) {
          throw new IllegalStateException(e);
        }
      }
    }
    return out;
  }

  private JsonNode awaitEvent(String topic, UUID rfqId, String field, String value) {
    return await()
        .atMost(TIMEOUT)
        .until(
            () ->
                eventsFor(topic, rfqId).stream()
                    .filter(e -> value.equals(e.path(field).asText()))
                    .findFirst()
                    .orElse(null),
            e -> e != null);
  }

  private double ignoredCount() {
    return meters
        .find("workflow.events.ignored")
        .tag("topic", Topics.PRICING_RESPONSES)
        .counters()
        .stream()
        .mapToDouble(c -> c.count())
        .sum();
  }
}
