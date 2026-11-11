package com.shammykr.tradeworkflow.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Publishes JSON events keyed by RFQ id.
 *
 * <p>Inside a transaction the send is deferred until after commit, so consumers never see an event
 * for a state change that was rolled back (for example by an optimistic-lock conflict). The
 * trade-off: a crash between commit and send loses the event. A transactional outbox would close
 * that gap; see DESIGN.md.
 */
@Component
public class EventPublisher {

  private static final Logger log = LoggerFactory.getLogger(EventPublisher.class);

  private final KafkaTemplate<String, String> kafka;
  private final ObjectMapper json;

  public EventPublisher(KafkaTemplate<String, String> kafka, ObjectMapper json) {
    this.kafka = kafka;
    this.json = json;
  }

  public void publish(String topic, UUID rfqId, Object event) {
    String payload;
    try {
      payload = json.writeValueAsString(event);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("cannot serialize " + event.getClass().getSimpleName(), e);
    }
    String key = rfqId.toString();
    if (TransactionSynchronizationManager.isSynchronizationActive()) {
      TransactionSynchronizationManager.registerSynchronization(
          new TransactionSynchronization() {
            @Override
            public void afterCommit() {
              send(topic, key, payload);
            }
          });
    } else {
      send(topic, key, payload);
    }
  }

  private void send(String topic, String key, String payload) {
    kafka
        .send(topic, key, payload)
        .whenComplete(
            (result, error) -> {
              if (error != null) {
                log.atError()
                    .setMessage("event publish failed")
                    .addKeyValue("topic", topic)
                    .addKeyValue("rfqId", key)
                    .setCause(error)
                    .log();
              } else {
                log.atDebug()
                    .setMessage("event published")
                    .addKeyValue("topic", topic)
                    .addKeyValue("rfqId", key)
                    .addKeyValue("offset", result.getRecordMetadata().offset())
                    .log();
              }
            });
  }
}
