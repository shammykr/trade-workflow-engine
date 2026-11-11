package com.shammykr.tradeworkflow.workflow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** Periodically rejects RFQs whose quote expired before the client acted. */
@Component
public class QuoteExpiryScheduler {

  private static final Logger log = LoggerFactory.getLogger(QuoteExpiryScheduler.class);

  private final RfqWorkflowService workflow;

  public QuoteExpiryScheduler(RfqWorkflowService workflow) {
    this.workflow = workflow;
  }

  @Scheduled(fixedDelayString = "${workflow.expiry-check-interval}")
  public void sweep() {
    int expired = workflow.expireQuotes();
    if (expired > 0) {
      log.atInfo().setMessage("expired quotes").addKeyValue("count", expired).log();
    }
  }
}
