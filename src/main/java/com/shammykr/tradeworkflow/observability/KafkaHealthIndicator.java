package com.shammykr.tradeworkflow.observability;

import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

/** Adds a {@code kafka} component to /actuator/health: up if the cluster answers within 3 s. */
@Component("kafka")
public class KafkaHealthIndicator implements HealthIndicator, DisposableBean {

  private final AdminClient admin;

  public KafkaHealthIndicator(KafkaAdmin kafkaAdmin) {
    this.admin = AdminClient.create(kafkaAdmin.getConfigurationProperties());
  }

  @Override
  public Health health() {
    try {
      var cluster = admin.describeCluster(new DescribeClusterOptions().timeoutMs(3000));
      String clusterId = cluster.clusterId().get(3, TimeUnit.SECONDS);
      int nodes = cluster.nodes().get(3, TimeUnit.SECONDS).size();
      return Health.up().withDetail("clusterId", clusterId).withDetail("nodes", nodes).build();
    } catch (Exception e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      return Health.down(e).build();
    }
  }

  @Override
  public void destroy() {
    admin.close();
  }
}
