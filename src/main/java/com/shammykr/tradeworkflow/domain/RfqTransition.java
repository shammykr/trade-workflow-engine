package com.shammykr.tradeworkflow.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import java.time.Instant;

/** Append-only audit record of one state change. */
@Entity
@Table(name = "rfq_transition")
public class RfqTransition {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  @ManyToOne(fetch = FetchType.LAZY, optional = false)
  @JoinColumn(name = "rfq_id")
  private Rfq rfq;

  @Enumerated(EnumType.STRING)
  @Column(name = "from_status")
  private RfqStatus fromStatus;

  @Enumerated(EnumType.STRING)
  @Column(name = "to_status", nullable = false)
  private RfqStatus toStatus;

  private String detail;

  @Column(nullable = false)
  private String actor;

  @Column(name = "occurred_at", nullable = false)
  private Instant occurredAt;

  protected RfqTransition() {}

  RfqTransition(
      Rfq rfq, RfqStatus from, RfqStatus to, String detail, String actor, Instant occurredAt) {
    this.rfq = rfq;
    this.fromStatus = from;
    this.toStatus = to;
    this.detail = detail;
    this.actor = actor;
    this.occurredAt = occurredAt;
  }

  public Long getId() {
    return id;
  }

  public RfqStatus getFromStatus() {
    return fromStatus;
  }

  public RfqStatus getToStatus() {
    return toStatus;
  }

  public String getDetail() {
    return detail;
  }

  public String getActor() {
    return actor;
  }

  public Instant getOccurredAt() {
    return occurredAt;
  }
}
