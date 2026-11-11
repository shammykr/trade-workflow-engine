package com.shammykr.tradeworkflow.domain;

import java.util.EnumSet;
import java.util.Set;

/**
 * RFQ lifecycle.
 *
 * <pre>
 *   PENDING ──price received──► QUOTED ──client accepts──► ACCEPTED ──booking confirmed──► BOOKED
 *      │                          │                           │
 *      │ pricing refused /        │ client declines /         │ booking rejected
 *      │ pricing unavailable      │ quote expired             │
 *      └──────────────► REJECTED ◄┴───────────────────────────┘
 * </pre>
 *
 * BOOKED and REJECTED are terminal.
 */
public enum RfqStatus {
  PENDING,
  QUOTED,
  ACCEPTED,
  BOOKED,
  REJECTED;

  public Set<RfqStatus> allowedNext() {
    return switch (this) {
      case PENDING -> EnumSet.of(QUOTED, REJECTED);
      case QUOTED -> EnumSet.of(ACCEPTED, REJECTED);
      case ACCEPTED -> EnumSet.of(BOOKED, REJECTED);
      case BOOKED, REJECTED -> EnumSet.noneOf(RfqStatus.class);
    };
  }

  public boolean canTransitionTo(RfqStatus next) {
    return allowedNext().contains(next);
  }

  public boolean isTerminal() {
    return allowedNext().isEmpty();
  }
}
