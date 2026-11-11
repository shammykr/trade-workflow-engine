package com.shammykr.tradeworkflow.config;

import com.shammykr.tradeworkflow.messaging.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConfig {

  private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);
  private static final int PARTITIONS = 3;

  @Bean
  NewTopic pricingResponsesTopic() {
    return TopicBuilder.name(Topics.PRICING_RESPONSES).partitions(PARTITIONS).replicas(1).build();
  }

  @Bean
  NewTopic quoteEventsTopic() {
    return TopicBuilder.name(Topics.QUOTE_EVENTS).partitions(PARTITIONS).replicas(1).build();
  }

  @Bean
  NewTopic bookingEventsTopic() {
    return TopicBuilder.name(Topics.BOOKING_EVENTS).partitions(PARTITIONS).replicas(1).build();
  }

  @Bean
  NewTopic bookingConfirmationsTopic() {
    return TopicBuilder.name(Topics.BOOKING_CONFIRMATIONS)
        .partitions(PARTITIONS)
        .replicas(1)
        .build();
  }

  /**
   * Listener failures (for example an optimistic-lock conflict with a concurrent desk action) are
   * retried 5 times, 200 ms apart, against fresh state. A record that still fails goes to {@code
   * <topic>.DLT} instead of blocking the partition.
   */
  @Bean
  DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> template) {
    DeadLetterPublishingRecoverer recoverer =
        new DeadLetterPublishingRecoverer(
            template,
            (record, ex) -> {
              log.atError()
                  .setMessage("record sent to dead-letter topic")
                  .addKeyValue("topic", record.topic())
                  .addKeyValue("key", record.key())
                  .setCause(ex)
                  .log();
              return new org.apache.kafka.common.TopicPartition(record.topic() + ".DLT", -1);
            });
    return new DefaultErrorHandler(recoverer, new FixedBackOff(200L, 5L));
  }
}
