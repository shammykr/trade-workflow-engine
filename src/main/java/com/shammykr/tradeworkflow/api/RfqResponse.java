package com.shammykr.tradeworkflow.api;

import com.shammykr.tradeworkflow.domain.ProductType;
import com.shammykr.tradeworkflow.domain.Quote;
import com.shammykr.tradeworkflow.domain.QuoteStatus;
import com.shammykr.tradeworkflow.domain.RejectionReason;
import com.shammykr.tradeworkflow.domain.Rfq;
import com.shammykr.tradeworkflow.domain.RfqStatus;
import com.shammykr.tradeworkflow.domain.RfqTransition;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record RfqResponse(
    UUID id,
    String clientId,
    String underlier,
    ProductType productType,
    BigDecimal notional,
    String currency,
    int maturityMonths,
    RfqStatus status,
    RejectionReason rejectionReason,
    String rejectionDetail,
    String tradeId,
    List<QuoteView> quotes,
    long version,
    Instant createdAt,
    Instant updatedAt) {

  public record QuoteView(
      UUID id, BigDecimal pricePct, QuoteStatus status, Instant validUntil, Instant createdAt) {

    static QuoteView of(Quote q) {
      return new QuoteView(
          q.getId(), q.getPricePct(), q.getStatus(), q.getValidUntil(), q.getCreatedAt());
    }
  }

  public record TransitionView(
      RfqStatus from, RfqStatus to, String detail, String actor, Instant at) {

    static TransitionView of(RfqTransition t) {
      return new TransitionView(
          t.getFromStatus(), t.getToStatus(), t.getDetail(), t.getActor(), t.getOccurredAt());
    }
  }

  static RfqResponse of(Rfq r) {
    return new RfqResponse(
        r.getId(),
        r.getClientId(),
        r.getUnderlier(),
        r.getProductType(),
        r.getNotional(),
        r.getCurrency(),
        r.getMaturityMonths(),
        r.getStatus(),
        r.getRejectionReason(),
        r.getRejectionDetail(),
        r.getTradeId(),
        r.getQuotes().stream().map(QuoteView::of).toList(),
        r.getVersion(),
        r.getCreatedAt(),
        r.getUpdatedAt());
  }
}
