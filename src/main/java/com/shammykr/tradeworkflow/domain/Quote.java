package com.shammykr.tradeworkflow.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/** A firm price for an RFQ, valid until {@link #getValidUntil()}. */
@Entity
@Table(name = "quote")
public class Quote {

  @Id private UUID id;

  @ManyToOne(fetch = FetchType.LAZY, optional = false)
  @JoinColumn(name = "rfq_id")
  private Rfq rfq;

  /** Price as a percentage of notional, e.g. 98.75. */
  @Column(name = "price_pct", nullable = false, precision = 9, scale = 4)
  private BigDecimal pricePct;

  @Column(name = "pricing_request_id", nullable = false)
  private String pricingRequestId;

  @Enumerated(EnumType.STRING)
  @Column(nullable = false)
  private QuoteStatus status;

  @Column(name = "valid_until", nullable = false)
  private Instant validUntil;

  @Column(name = "created_at", nullable = false)
  private Instant createdAt;

  @Version private long version;

  protected Quote() {}

  Quote(Rfq rfq, BigDecimal pricePct, String pricingRequestId, Instant validUntil, Instant now) {
    this.id = UUID.randomUUID();
    this.rfq = rfq;
    this.pricePct = pricePct;
    this.pricingRequestId = pricingRequestId;
    this.status = QuoteStatus.ACTIVE;
    this.validUntil = validUntil;
    this.createdAt = now;
  }

  public boolean isExpiredAt(Instant now) {
    return !now.isBefore(validUntil);
  }

  public void setStatus(QuoteStatus status) {
    this.status = status;
  }

  public UUID getId() {
    return id;
  }

  public Rfq getRfq() {
    return rfq;
  }

  public BigDecimal getPricePct() {
    return pricePct;
  }

  public String getPricingRequestId() {
    return pricingRequestId;
  }

  public QuoteStatus getStatus() {
    return status;
  }

  public Instant getValidUntil() {
    return validUntil;
  }

  public Instant getCreatedAt() {
    return createdAt;
  }

  public long getVersion() {
    return version;
  }
}
