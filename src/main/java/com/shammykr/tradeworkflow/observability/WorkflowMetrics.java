package com.shammykr.tradeworkflow.observability;

import com.shammykr.tradeworkflow.domain.ProductType;
import com.shammykr.tradeworkflow.domain.RejectionReason;
import com.shammykr.tradeworkflow.domain.RfqStatus;
import com.shammykr.tradeworkflow.repository.RfqRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.time.Instant;
import org.springframework.stereotype.Component;

/**
 * Workflow metrics (exposed at /actuator/metrics and /actuator/prometheus).
 *
 * <ul>
 *   <li>{@code rfq.submitted{product}}: RFQs received
 *   <li>{@code rfq.transitions{from,to}}: state machine transitions
 *   <li>{@code rfq.rejected{reason}}: rejections by reason
 *   <li>{@code rfq.time.to.quote}, {@code rfq.time.to.book}: latency from submission
 *   <li>{@code rfq.status.count{status}}: RFQs currently in each state (gauge, from the DB)
 *   <li>{@code workflow.events.ignored{topic,reason}}: duplicate or stale Kafka messages skipped
 *   <li>{@code workflow.optimistic.lock.conflicts{operation}}: concurrent-update conflicts
 * </ul>
 */
@Component
public class WorkflowMetrics {

  private final MeterRegistry registry;

  public WorkflowMetrics(MeterRegistry registry, RfqRepository rfqs) {
    this.registry = registry;
    for (RfqStatus status : RfqStatus.values()) {
      Gauge.builder("rfq.status.count", rfqs, r -> r.countByStatus(status))
          .description("RFQs currently in this state")
          .tag("status", status.name())
          .register(registry);
    }
  }

  public void submitted(ProductType product) {
    Counter.builder("rfq.submitted").tag("product", product.name()).register(registry).increment();
  }

  public void transition(RfqStatus from, RfqStatus to) {
    Counter.builder("rfq.transitions")
        .tag("from", from == null ? "NONE" : from.name())
        .tag("to", to.name())
        .register(registry)
        .increment();
  }

  public void rejected(RejectionReason reason) {
    Counter.builder("rfq.rejected").tag("reason", reason.name()).register(registry).increment();
  }

  public void timeToQuote(Instant submittedAt, Instant now) {
    Timer.builder("rfq.time.to.quote")
        .publishPercentileHistogram()
        .register(registry)
        .record(Duration.between(submittedAt, now));
  }

  public void timeToBook(Instant submittedAt, Instant now) {
    Timer.builder("rfq.time.to.book")
        .publishPercentileHistogram()
        .register(registry)
        .record(Duration.between(submittedAt, now));
  }

  public void ignoredEvent(String topic, String reason) {
    Counter.builder("workflow.events.ignored")
        .tag("topic", topic)
        .tag("reason", reason)
        .register(registry)
        .increment();
  }

  public void optimisticLockConflict(String operation) {
    Counter.builder("workflow.optimistic.lock.conflicts")
        .tag("operation", operation)
        .register(registry)
        .increment();
  }
}
