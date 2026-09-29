package com.taskforge.common;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.KafkaFuture;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

class BrokerHealthIndicatorTest {
    @Test void apiRoleDoesNotRequireKafkaBecauseOutboxIsDurable() {
        BrokerHealthIndicator indicator = new BrokerHealthIndicator((AdminClient) null, "api");

        assertEquals(org.springframework.boot.actuate.health.Status.UP, indicator.health().getStatus());
    }

    @Test void workerRoleReportsKafkaAvailability() throws Exception {
        AdminClient admin = mock(AdminClient.class);
        DescribeClusterResult cluster = mock(DescribeClusterResult.class);
        when(admin.describeCluster()).thenReturn(cluster);
        when(cluster.nodes()).thenReturn(KafkaFuture.completedFuture(List.of(new Node(1, "localhost", 9092))));
        BrokerHealthIndicator indicator = new BrokerHealthIndicator(admin, "worker");

        assertEquals(org.springframework.boot.actuate.health.Status.UP, indicator.health().getStatus());
        verify(admin).describeCluster();
    }

    @Test void workerRoleReportsBrokerFailureAsDown() {
        AdminClient admin = mock(AdminClient.class);
        when(admin.describeCluster()).thenThrow(new IllegalStateException("broker unavailable"));
        BrokerHealthIndicator indicator = new BrokerHealthIndicator(admin, "worker");

        assertEquals(org.springframework.boot.actuate.health.Status.DOWN, indicator.health().getStatus());
    }

    @Test void workerRoleReportsEmptyBrokerListAsDown() throws Exception {
        AdminClient admin = mock(AdminClient.class);
        DescribeClusterResult cluster = mock(DescribeClusterResult.class);
        when(admin.describeCluster()).thenReturn(cluster);
        when(cluster.nodes()).thenReturn(KafkaFuture.completedFuture(List.<Node>of()));
        BrokerHealthIndicator indicator = new BrokerHealthIndicator(admin, "worker");

        assertEquals(org.springframework.boot.actuate.health.Status.DOWN, indicator.health().getStatus());
    }
}
