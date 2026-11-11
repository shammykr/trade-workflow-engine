package com.shammykr.tradeworkflow.integration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shammykr.tradeworkflow.messaging.BookingConfirmation;
import com.shammykr.tradeworkflow.messaging.PricingResponse;
import com.shammykr.tradeworkflow.messaging.Topics;
import com.shammykr.tradeworkflow.workflow.RfqWorkflowService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Inbound Kafka integration: prices from the pricing service, answers from the booking system. The
 * topic and offset go into the MDC so every log line written while handling a message carries them
 * (the RFQ id is already a structured field on the workflow's own log lines).
 */
@Component
public class WorkflowKafkaListeners {

  private final RfqWorkflowService workflow;
  private final ObjectMapper json;

  public WorkflowKafkaListeners(RfqWorkflowService workflow, ObjectMapper json) {
    this.workflow = workflow;
    this.json = json;
  }

  @KafkaListener(topics = Topics.PRICING_RESPONSES, groupId = "${spring.kafka.consumer.group-id}")
  public void onPricingResponse(ConsumerRecord<String, String> record)
      throws JsonProcessingException {
    MDC.put("kafkaTopic", record.topic());
    MDC.put("kafkaOffset", Long.toString(record.offset()));
    try {
      workflow.onPricingResponse(json.readValue(record.value(), PricingResponse.class));
    } finally {
      MDC.remove("kafkaTopic");
      MDC.remove("kafkaOffset");
    }
  }

  @KafkaListener(
      topics = Topics.BOOKING_CONFIRMATIONS,
      groupId = "${spring.kafka.consumer.group-id}")
  public void onBookingConfirmation(ConsumerRecord<String, String> record)
      throws JsonProcessingException {
    MDC.put("kafkaTopic", record.topic());
    MDC.put("kafkaOffset", Long.toString(record.offset()));
    try {
      workflow.onBookingConfirmation(json.readValue(record.value(), BookingConfirmation.class));
    } finally {
      MDC.remove("kafkaTopic");
      MDC.remove("kafkaOffset");
    }
  }
}
