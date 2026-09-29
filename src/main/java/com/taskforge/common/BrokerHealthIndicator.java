package com.taskforge.common;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Component("brokerHealthIndicator")
public class BrokerHealthIndicator implements HealthIndicator, AutoCloseable {
    private final boolean required;
    private final AdminClient admin;

    public BrokerHealthIndicator(KafkaAdmin kafkaAdmin, @Value("${taskforge.role:api}") String role) {
        this.required = "worker".equals(role);
        if (required) {
            Map<String, Object> config = new HashMap<>(kafkaAdmin.getConfigurationProperties());
            config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 1_000);
            config.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 1_500);
            this.admin = AdminClient.create(config);
        } else {
            this.admin = null;
        }
    }

    BrokerHealthIndicator(AdminClient admin, String role) {
        this.required = "worker".equals(role);
        this.admin = admin;
    }

    @Override
    public Health health() {
        if (!required) return Health.up().withDetail("required", false).build();
        try {
            if (admin.describeCluster().nodes().get(2, TimeUnit.SECONDS).isEmpty()) {
                return Health.down().withDetail("broker", "no brokers discovered").build();
            }
            return Health.up().build();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return Health.down().withDetail("broker", "unavailable").build();
        } catch (Exception unavailable) {
            return Health.down().withDetail("broker", "unavailable").build();
        }
    }

    @Override
    public void close() {
        if (admin != null) admin.close(Duration.ofMillis(250));
    }
}
