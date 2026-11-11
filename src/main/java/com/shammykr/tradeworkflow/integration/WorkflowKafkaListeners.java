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

/** Inbound Kafka integration: prices from the pricing service, answers from the booking system. */
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
    MDC.put("rfqId", record.key());
    try {
      workflow.onPricingResponse(json.readValue(record.value(), PricingResponse.class));
    } finally {
      MDC.remove("rfqId");
    }
  }

  @KafkaListener(
      topics = Topics.BOOKING_CONFIRMATIONS,
      groupId = "${spring.kafka.consumer.group-id}")
  public void onBookingConfirmation(ConsumerRecord<String, String> record)
      throws JsonProcessingException {
    MDC.put("rfqId", record.key());
    try {
      workflow.onBookingConfirmation(json.readValue(record.value(), BookingConfirmation.class));
    } finally {
      MDC.remove("rfqId");
    }
  }
}
