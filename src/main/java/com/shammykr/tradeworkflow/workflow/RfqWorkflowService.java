package com.shammykr.tradeworkflow.workflow;

import com.shammykr.tradeworkflow.config.WorkflowProperties;
import com.shammykr.tradeworkflow.domain.InvalidTransitionException;
import com.shammykr.tradeworkflow.domain.Quote;
import com.shammykr.tradeworkflow.domain.QuoteStatus;
import com.shammykr.tradeworkflow.domain.RejectionReason;
import com.shammykr.tradeworkflow.domain.Rfq;
import com.shammykr.tradeworkflow.domain.RfqStatus;
import com.shammykr.tradeworkflow.messaging.BookingConfirmation;
import com.shammykr.tradeworkflow.messaging.BookingEvent;
import com.shammykr.tradeworkflow.messaging.EventPublisher;
import com.shammykr.tradeworkflow.messaging.PricingRequest;
import com.shammykr.tradeworkflow.messaging.PricingResponse;
import com.shammykr.tradeworkflow.messaging.QuoteEvent;
import com.shammykr.tradeworkflow.messaging.Topics;
import com.shammykr.tradeworkflow.observability.WorkflowMetrics;
import com.shammykr.tradeworkflow.repository.RfqRepository;
import java.time.Clock;
import java.time.Instant;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Orchestrates the RFQ → quote → booking workflow.
 *
 * <p>Every state change runs in its own short transaction and goes through {@link
 * Rfq#transitionTo}, which enforces the state machine and appends to the audit trail. Events are
 * published after commit (see {@link EventPublisher}). Kafka handlers are idempotent: a message
 * that doesn't match the RFQ's current state (a duplicate, or one that lost a race) is logged,
 * counted and ignored.
 */
@Service
public class RfqWorkflowService {

  private static final Logger log = LoggerFactory.getLogger(RfqWorkflowService.class);
  private static final String SYSTEM = "system";

  private final RfqRepository rfqs;
  private final PricingGateway pricing;
  private final EventPublisher events;
  private final WorkflowMetrics metrics;
  private final WorkflowProperties props;
  private final TransactionTemplate tx;
  private final Clock clock;

  public RfqWorkflowService(
      RfqRepository rfqs,
      PricingGateway pricing,
      EventPublisher events,
      WorkflowMetrics metrics,
      WorkflowProperties props,
      TransactionTemplate tx,
      Clock clock) {
    this.rfqs = rfqs;
    this.pricing = pricing;
    this.events = events;
    this.metrics = metrics;
    this.props = props;
    this.tx = tx;
    this.clock = clock;
  }

  // ---- RFQ submission ------------------------------------------------------------------------

  /**
   * Stores the RFQ as PENDING and requests a price over REST. The price arrives asynchronously on
   * {@code pricing-responses}. If the pricing service can't be reached the RFQ is rejected with
   * PRICING_UNAVAILABLE.
   */
  public Rfq submit(SubmitRfqCommand cmd, String actor) {
    Instant now = clock.instant();
    String pricingRequestId = "PR-" + UUID.randomUUID();
    Rfq saved =
        tx.execute(
            status -> {
              Rfq rfq =
                  new Rfq(
                      cmd.clientId(),
                      cmd.underlier(),
                      cmd.productType(),
                      cmd.notional(),
                      cmd.currency(),
                      cmd.maturityMonths(),
                      actor,
                      now);
              rfq.setPricingRequestId(pricingRequestId);
              return rfqs.save(rfq);
            });
    metrics.submitted(cmd.productType());
    metrics.transition(null, RfqStatus.PENDING);
    log.atInfo()
        .setMessage("RFQ submitted")
        .addKeyValue("rfqId", saved.getId())
        .addKeyValue("clientId", saved.getClientId())
        .addKeyValue("product", saved.getProductType())
        .addKeyValue("notional", saved.getNotional())
        .log();

    try {
      pricing.requestPrice(
          new PricingRequest(
              pricingRequestId,
              saved.getId(),
              saved.getUnderlier(),
              saved.getProductType(),
              saved.getNotional(),
              saved.getCurrency(),
              saved.getMaturityMonths()));
      return saved;
    } catch (PricingGateway.PricingUnavailableException e) {
      log.atWarn()
          .setMessage("pricing service unavailable, rejecting RFQ")
          .addKeyValue("rfqId", saved.getId())
          .setCause(e)
          .log();
      return update(
          saved.getId(),
          "pricing-unavailable",
          rfq -> {
            if (rfq.getStatus() == RfqStatus.PENDING) {
              reject(rfq, RejectionReason.PRICING_UNAVAILABLE, e.getMessage(), SYSTEM);
            }
          });
    }
  }

  // ---- pricing-responses ---------------------------------------------------------------------

  public void onPricingResponse(PricingResponse response) {
    update(
        response.rfqId(),
        "pricing-response",
        rfq -> {
          if (rfq.getStatus() != RfqStatus.PENDING
              || !response.pricingRequestId().equals(rfq.getPricingRequestId())) {
            ignore(Topics.PRICING_RESPONSES, rfq, "not awaiting this pricing request");
            return;
          }
          Instant now = clock.instant();
          if (response.status() == PricingResponse.Status.REFUSED) {
            reject(rfq, RejectionReason.PRICING_REFUSED, response.reason(), "pricing-service");
            return;
          }
          Quote quote =
              rfq.addQuote(
                  response.pricePct(),
                  response.pricingRequestId(),
                  now.plus(props.quoteTtl()),
                  now);
          transition(
              rfq,
              RfqStatus.QUOTED,
              "quote " + quote.getId() + " @ " + quote.getPricePct(),
              "pricing-service");
          metrics.timeToQuote(rfq.getCreatedAt(), now);
          events.publish(
              Topics.QUOTE_EVENTS,
              rfq.getId(),
              quoteEvent(QuoteEvent.Type.QUOTE_CREATED, rfq, quote));
          log.atInfo()
              .setMessage("quote created")
              .addKeyValue("rfqId", rfq.getId())
              .addKeyValue("quoteId", quote.getId())
              .addKeyValue("pricePct", quote.getPricePct())
              .addKeyValue("validUntil", quote.getValidUntil())
              .log();
        });
  }

  // ---- desk / client actions -----------------------------------------------------------------

  /**
   * Accepts the RFQ's active quote and asks the booking system to book it.
   *
   * @param expectedVersion RFQ version the caller last saw (If-Match), or null to skip the check
   */
  public Rfq accept(UUID rfqId, UUID quoteId, Long expectedVersion, String actor) {
    return tx.execute(
        status -> {
          Rfq rfq = load(rfqId);
          checkVersion(rfq, expectedVersion);
          Quote quote = activeQuote(rfq, quoteId);
          Instant now = clock.instant();
          if (quote.isExpiredAt(now)) {
            throw new QuoteExpiredException(rfqId, quoteId);
          }
          quote.setStatus(QuoteStatus.ACCEPTED);
          transition(rfq, RfqStatus.ACCEPTED, "quote " + quoteId + " accepted", actor);
          events.publish(
              Topics.QUOTE_EVENTS, rfqId, quoteEvent(QuoteEvent.Type.QUOTE_ACCEPTED, rfq, quote));
          events.publish(
              Topics.BOOKING_EVENTS,
              rfqId,
              new BookingEvent(
                  UUID.randomUUID(),
                  BookingEvent.Type.BOOKING_REQUESTED,
                  rfqId,
                  quoteId,
                  rfq.getClientId(),
                  rfq.getUnderlier(),
                  rfq.getProductType(),
                  rfq.getNotional(),
                  rfq.getCurrency(),
                  rfq.getMaturityMonths(),
                  quote.getPricePct(),
                  now));
          // Flush inside the transaction so a concurrent update surfaces here as an
          // optimistic-lock failure rather than at an arbitrary later point.
          rfqs.saveAndFlush(rfq);
          log.atInfo()
              .setMessage("quote accepted, booking requested")
              .addKeyValue("rfqId", rfqId)
              .addKeyValue("quoteId", quoteId)
              .addKeyValue("actor", actor)
              .log();
          return rfq;
        });
  }

  /** The client declines the quote (or the desk withdraws it). */
  public Rfq decline(UUID rfqId, UUID quoteId, Long expectedVersion, String actor) {
    return tx.execute(
        status -> {
          Rfq rfq = load(rfqId);
          checkVersion(rfq, expectedVersion);
          Quote quote = activeQuote(rfq, quoteId);
          quote.setStatus(QuoteStatus.DECLINED);
          reject(rfq, RejectionReason.CLIENT_DECLINED, "declined by " + actor, actor);
          events.publish(
              Topics.QUOTE_EVENTS, rfqId, quoteEvent(QuoteEvent.Type.QUOTE_DECLINED, rfq, quote));
          rfqs.saveAndFlush(rfq);
          return rfq;
        });
  }

  // ---- booking-confirmations -----------------------------------------------------------------

  public void onBookingConfirmation(BookingConfirmation confirmation) {
    update(
        confirmation.rfqId(),
        "booking-confirmation",
        rfq -> {
          boolean expected =
              rfq.getStatus() == RfqStatus.ACCEPTED
                  && rfq.quote(confirmation.quoteId())
                      .map(q -> q.getStatus() == QuoteStatus.ACCEPTED)
                      .orElse(false);
          if (!expected) {
            ignore(Topics.BOOKING_CONFIRMATIONS, rfq, "not awaiting booking of this quote");
            return;
          }
          Instant now = clock.instant();
          if (confirmation.status() == BookingConfirmation.Status.REJECTED) {
            reject(rfq, RejectionReason.BOOKING_REJECTED, confirmation.reason(), "booking-system");
            return;
          }
          rfq.markBooked(confirmation.tradeId(), "booking-system", now);
          metrics.transition(RfqStatus.ACCEPTED, RfqStatus.BOOKED);
          metrics.timeToBook(rfq.getCreatedAt(), now);
          log.atInfo()
              .setMessage("RFQ booked")
              .addKeyValue("rfqId", rfq.getId())
              .addKeyValue("tradeId", confirmation.tradeId())
              .log();
        });
  }

  // ---- quote expiry --------------------------------------------------------------------------

  /** Rejects QUOTED RFQs whose quote has expired. Called by {@link QuoteExpiryScheduler}. */
  public int expireQuotes() {
    int expired = 0;
    for (UUID id : rfqs.findIdsWithExpiredQuotes(clock.instant())) {
      try {
        Boolean done =
            tx.execute(
                status -> {
                  Rfq rfq = load(id);
                  Instant now = clock.instant();
                  var quote = rfq.activeQuote().filter(q -> q.isExpiredAt(now));
                  if (rfq.getStatus() != RfqStatus.QUOTED || quote.isEmpty()) {
                    return false; // accepted or declined in the meantime
                  }
                  quote.get().setStatus(QuoteStatus.EXPIRED);
                  reject(
                      rfq,
                      RejectionReason.QUOTE_EXPIRED,
                      "quote " + quote.get().getId() + " expired",
                      SYSTEM);
                  events.publish(
                      Topics.QUOTE_EVENTS,
                      id,
                      quoteEvent(QuoteEvent.Type.QUOTE_EXPIRED, rfq, quote.get()));
                  return true;
                });
        if (Boolean.TRUE.equals(done)) {
          expired++;
        }
      } catch (ObjectOptimisticLockingFailureException e) {
        // A desk action on the same RFQ committed first; it wins and the next sweep re-checks.
        metrics.optimisticLockConflict("expire-quote");
      }
    }
    return expired;
  }

  // ---- helpers -------------------------------------------------------------------------------

  /** Loads, mutates and commits one RFQ; conflicts are counted and rethrown for retry. */
  private Rfq update(UUID rfqId, String operation, java.util.function.Consumer<Rfq> change) {
    try {
      return tx.execute(
          status -> {
            Rfq rfq = load(rfqId);
            change.accept(rfq);
            return rfqs.saveAndFlush(rfq);
          });
    } catch (ObjectOptimisticLockingFailureException e) {
      metrics.optimisticLockConflict(operation);
      throw e;
    }
  }

  private Rfq load(UUID id) {
    return rfqs.findById(id).orElseThrow(() -> new RfqNotFoundException(id));
  }

  private static void checkVersion(Rfq rfq, Long expectedVersion) {
    if (expectedVersion != null && expectedVersion != rfq.getVersion()) {
      throw new StaleVersionException(rfq.getId(), expectedVersion, rfq.getVersion());
    }
  }

  private static Quote activeQuote(Rfq rfq, UUID quoteId) {
    if (rfq.getStatus() != RfqStatus.QUOTED) {
      throw new InvalidTransitionException(
          "RFQ "
              + rfq.getId()
              + " is "
              + rfq.getStatus()
              + "; only QUOTED RFQs can be accepted or declined");
    }
    Quote quote =
        rfq.quote(quoteId)
            .orElseThrow(
                () ->
                    new InvalidTransitionException(
                        "quote " + quoteId + " does not belong to RFQ " + rfq.getId()));
    if (quote.getStatus() != QuoteStatus.ACTIVE) {
      throw new InvalidTransitionException("quote " + quoteId + " is " + quote.getStatus());
    }
    return quote;
  }

  private void transition(Rfq rfq, RfqStatus next, String detail, String actor) {
    RfqStatus from = rfq.getStatus();
    rfq.transitionTo(next, detail, actor, clock.instant());
    metrics.transition(from, next);
  }

  private void reject(Rfq rfq, RejectionReason reason, String detail, String actor) {
    RfqStatus from = rfq.getStatus();
    rfq.reject(reason, detail, actor, clock.instant());
    metrics.transition(from, RfqStatus.REJECTED);
    metrics.rejected(reason);
    log.atInfo()
        .setMessage("RFQ rejected")
        .addKeyValue("rfqId", rfq.getId())
        .addKeyValue("from", from)
        .addKeyValue("reason", reason)
        .addKeyValue("detail", detail)
        .log();
  }

  private void ignore(String topic, Rfq rfq, String why) {
    metrics.ignoredEvent(topic, why.replace(' ', '_'));
    log.atInfo()
        .setMessage("ignoring message")
        .addKeyValue("topic", topic)
        .addKeyValue("rfqId", rfq.getId())
        .addKeyValue("status", rfq.getStatus())
        .addKeyValue("why", why)
        .log();
  }

  private static QuoteEvent quoteEvent(QuoteEvent.Type type, Rfq rfq, Quote quote) {
    return new QuoteEvent(
        UUID.randomUUID(),
        type,
        rfq.getId(),
        quote.getId(),
        rfq.getClientId(),
        rfq.getUnderlier(),
        rfq.getProductType(),
        rfq.getNotional(),
        rfq.getCurrency(),
        quote.getPricePct(),
        quote.getValidUntil(),
        Instant.now());
  }
}
