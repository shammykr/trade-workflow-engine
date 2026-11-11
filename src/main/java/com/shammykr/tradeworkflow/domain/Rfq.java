package com.shammykr.tradeworkflow.domain;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.OneToMany;
import jakarta.persistence.OrderBy;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * A client's request for quote and the workflow state around it.
 *
 * <p>{@code @Version} gives optimistic locking: two desk users (or a user and a Kafka consumer)
 * acting on the same RFQ concurrently both read version N; the first commit bumps it to N+1 and the
 * second fails with an optimistic-lock exception instead of silently overwriting.
 */
@Entity
@Table(name = "rfq")
public class Rfq {

  @Id private UUID id;

  @Column(name = "client_id", nullable = false)
  private String clientId;

  @Column(nullable = false)
  private String underlier;

  @Enumerated(EnumType.STRING)
  @Column(name = "product_type", nullable = false)
  private ProductType productType;

  @Column(nullable = false, precision = 19, scale = 2)
  private BigDecimal notional;

  @Column(nullable = false, length = 3)
  private String currency;

  @Column(name = "maturity_months", nullable = false)
  private int maturityMonths;

  @Enumerated(EnumType.STRING)
  @Column(nullable = false)
  private RfqStatus status;

  @Enumerated(EnumType.STRING)
  @Column(name = "rejection_reason")
  private RejectionReason rejectionReason;

  @Column(name = "rejection_detail")
  private String rejectionDetail;

  @Column(name = "pricing_request_id")
  private String pricingRequestId;

  @Column(name = "trade_id")
  private String tradeId;

  @Column(name = "created_at", nullable = false)
  private Instant createdAt;

  @Column(name = "updated_at", nullable = false)
  private Instant updatedAt;

  @Version private long version;

  @OneToMany(mappedBy = "rfq", cascade = CascadeType.ALL, orphanRemoval = true)
  @OrderBy("createdAt ASC")
  private List<Quote> quotes = new ArrayList<>();

  @OneToMany(mappedBy = "rfq", cascade = CascadeType.ALL, orphanRemoval = true)
  @OrderBy("id ASC")
  private List<RfqTransition> transitions = new ArrayList<>();

  protected Rfq() {}

  public Rfq(
      String clientId,
      String underlier,
      ProductType productType,
      BigDecimal notional,
      String currency,
      int maturityMonths,
      String actor,
      Instant now) {
    this.id = UUID.randomUUID();
    this.clientId = clientId;
    this.underlier = underlier;
    this.productType = productType;
    this.notional = notional;
    this.currency = currency;
    this.maturityMonths = maturityMonths;
    this.status = RfqStatus.PENDING;
    this.createdAt = now;
    this.updatedAt = now;
    this.transitions.add(
        new RfqTransition(this, null, RfqStatus.PENDING, "RFQ submitted", actor, now));
  }

  /** Moves to {@code next} if the state machine allows it and records the transition. */
  public void transitionTo(RfqStatus next, String detail, String actor, Instant now) {
    if (!status.canTransitionTo(next)) {
      throw new InvalidTransitionException(id, status, next);
    }
    transitions.add(new RfqTransition(this, status, next, detail, actor, now));
    this.status = next;
    this.updatedAt = now;
  }

  public void reject(RejectionReason reason, String detail, String actor, Instant now) {
    transitionTo(RfqStatus.REJECTED, reason + (detail == null ? "" : ": " + detail), actor, now);
    this.rejectionReason = reason;
    this.rejectionDetail = detail;
  }

  public Quote addQuote(
      BigDecimal pricePct, String pricingRequestId, Instant validUntil, Instant now) {
    Quote quote = new Quote(this, pricePct, pricingRequestId, validUntil, now);
    quotes.add(quote);
    return quote;
  }

  public Optional<Quote> activeQuote() {
    return quotes.stream().filter(q -> q.getStatus() == QuoteStatus.ACTIVE).findFirst();
  }

  public Optional<Quote> quote(UUID quoteId) {
    return quotes.stream().filter(q -> q.getId().equals(quoteId)).findFirst();
  }

  public void markBooked(String tradeId, String actor, Instant now) {
    transitionTo(RfqStatus.BOOKED, "trade " + tradeId, actor, now);
    this.tradeId = tradeId;
  }

  public void setPricingRequestId(String pricingRequestId) {
    this.pricingRequestId = pricingRequestId;
  }

  public UUID getId() {
    return id;
  }

  public String getClientId() {
    return clientId;
  }

  public String getUnderlier() {
    return underlier;
  }

  public ProductType getProductType() {
    return productType;
  }

  public BigDecimal getNotional() {
    return notional;
  }

  public String getCurrency() {
    return currency;
  }

  public int getMaturityMonths() {
    return maturityMonths;
  }

  public RfqStatus getStatus() {
    return status;
  }

  public RejectionReason getRejectionReason() {
    return rejectionReason;
  }

  public String getRejectionDetail() {
    return rejectionDetail;
  }

  public String getPricingRequestId() {
    return pricingRequestId;
  }

  public String getTradeId() {
    return tradeId;
  }

  public Instant getCreatedAt() {
    return createdAt;
  }

  public Instant getUpdatedAt() {
    return updatedAt;
  }

  public long getVersion() {
    return version;
  }

  public List<Quote> getQuotes() {
    return quotes;
  }

  public List<RfqTransition> getTransitions() {
    return transitions;
  }
}
