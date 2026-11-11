package com.shammykr.tradeworkflow.stub;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shammykr.tradeworkflow.messaging.BookingConfirmation;
import com.shammykr.tradeworkflow.messaging.BookingEvent;
import com.shammykr.tradeworkflow.messaging.EventPublisher;
import com.shammykr.tradeworkflow.messaging.Topics;
import jakarta.annotation.PreDestroy;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Simulated downstream booking system. Consumes {@code booking-events} in its own consumer group
 * and, after a short latency, answers on {@code booking-confirmations}: CONFIRMED with a trade id,
 * or REJECTED for clients whose id starts with the blocked prefix (simulating a failed KYC check).
 */
@Component
@ConditionalOnProperty(prefix = "stubs", name = "enabled", havingValue = "true")
public class StubBookingSystem {

  private static final Logger log = LoggerFactory.getLogger(StubBookingSystem.class);

  private final EventPublisher events;
  private final ObjectMapper json;
  private final StubProperties.Booking props;
  private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

  public StubBookingSystem(EventPublisher events, ObjectMapper json, StubProperties props) {
    this.events = events;
    this.json = json;
    this.props = props.booking();
  }

  @KafkaListener(topics = Topics.BOOKING_EVENTS, groupId = "stub-booking-system")
  public void onBookingEvent(String payload) throws JsonProcessingException {
    BookingEvent event = json.readValue(payload, BookingEvent.class);
    if (event.type() != BookingEvent.Type.BOOKING_REQUESTED) {
      return;
    }
    executor.schedule(() -> confirm(event), props.latency().toMillis(), TimeUnit.MILLISECONDS);
  }

  private void confirm(BookingEvent event) {
    boolean blocked = event.clientId().startsWith(props.blockedClientPrefix());
    BookingConfirmation confirmation =
        blocked
            ? new BookingConfirmation(
                UUID.randomUUID(),
                event.rfqId(),
                event.quoteId(),
                BookingConfirmation.Status.REJECTED,
                null,
                "KYC check failed for client " + event.clientId(),
                Instant.now())
            : new BookingConfirmation(
                UUID.randomUUID(),
                event.rfqId(),
                event.quoteId(),
                BookingConfirmation.Status.CONFIRMED,
                "TRD-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase(),
                null,
                Instant.now());
    log.atInfo()
        .setMessage("stub booking system responded")
        .addKeyValue("rfqId", event.rfqId())
        .addKeyValue("status", confirmation.status())
        .addKeyValue("tradeId", confirmation.tradeId())
        .log();
    events.publish(Topics.BOOKING_CONFIRMATIONS, event.rfqId(), confirmation);
  }

  @PreDestroy
  void shutdown() {
    executor.shutdownNow();
  }
}
