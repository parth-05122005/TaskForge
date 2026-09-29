package com.taskforge;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.taskforge.auth.User;
import com.taskforge.auth.UserRepository;
import com.taskforge.job.*;
import com.taskforge.worker.Worker;
import com.taskforge.worker.WorkerRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import java.time.Instant;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@Testcontainers(disabledWithoutDocker=true)
class PlatformContainerIntegrationTest {
    @Container static final PostgreSQLContainer<?> postgres=new PostgreSQLContainer<>("postgres:17-alpine");
    @Container static final GenericContainer<?> redis=new GenericContainer<>(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);
    @Container static final KafkaContainer kafka=new KafkaContainer(DockerImageName.parse("apache/kafka-native:3.8.0"));

    @DynamicPropertySource static void infrastructure(DynamicPropertyRegistry properties){
        properties.add("spring.datasource.url",postgres::getJdbcUrl);
        properties.add("spring.datasource.username",postgres::getUsername);
        properties.add("spring.datasource.password",postgres::getPassword);
        properties.add("spring.data.redis.host",redis::getHost);
        properties.add("spring.data.redis.port",redis::getFirstMappedPort);
        properties.add("spring.kafka.bootstrap-servers",kafka::getBootstrapServers);
        properties.add("taskforge.jwt-secret",()->"integration-test-secret-with-more-than-thirty-two-bytes");
        properties.add("taskforge.role",()->"worker");
    }

    @Autowired JobRepository jobs;
    @Autowired JobExecutionRepository executions;
    @Autowired OutboxRepository outbox;
    @Autowired JobEventRepository events;
    @Autowired UserRepository users;
    @Autowired WorkerRepository workers;
    @Autowired KafkaTemplate<String,String> kafkaTemplate;
    @Autowired ObjectMapper mapper;
    @Autowired PlatformTransactionManager transactionManager;

    @Test void outboxDeliveryWorkerExecutionAndDuplicateKafkaDeliveryAreDurable() throws Exception {
        assertTrue(workers.count()>0,"worker runtime should register itself at startup");
        User owner=users.save(new User("platform-integration@example.com","test-hash"));
        Job job=jobs.save(new Job(owner,"container e2e",null,"REPORT","{\"reportType\":\"TEST\"}",ScheduleType.IMMEDIATE,null,Instant.now(),JobPriority.HIGH,2,30));
        JobEventService eventService=new JobEventService(events,outbox,mapper);
        Scheduler scheduler=new Scheduler(jobs,executions,outbox,eventService,mapper);
        TransactionTemplate transaction=new TransactionTemplate(transactionManager);
        transaction.execute(status->{scheduler.dispatchDue();return null;});

        transaction.execute(status->{new OutboxPublisher(outbox,kafkaTemplate,2,32).publishPending();return null;});
        awaitStatus(job.getId(),JobStatus.SUCCESS);
        JobExecution firstAttempt=executions.findByJobIdOrderByRunNumberDescAttemptNumberDesc(job.getId()).get(0);
        assertEquals(1,firstAttempt.getRunNumber());
        assertEquals(1,firstAttempt.getAttemptNumber());

        JobMessage duplicate=new JobMessage(job.getId(),firstAttempt.getId(),firstAttempt.getRunNumber(),job.getType(),job.getPayload(),job.getPriority().name(),firstAttempt.getAttemptNumber(),job.getTimeoutSeconds());
        var duplicateResult=kafkaTemplate.send("taskforge.jobs.execute",String.valueOf(job.getId()),mapper.writeValueAsString(duplicate)).get(10,TimeUnit.SECONDS);
        awaitKafkaConsumed(duplicateResult.getRecordMetadata().partition(),duplicateResult.getRecordMetadata().offset());

        assertEquals(JobStatus.SUCCESS,jobs.findById(job.getId()).orElseThrow().getStatus());
        assertEquals(1,executions.findByJobIdOrderByRunNumberDescAttemptNumberDesc(job.getId()).size());
    }

    private void awaitStatus(Long jobId,JobStatus expected)throws InterruptedException {
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(20);
        while(System.nanoTime()<deadline){
            if(jobs.findById(jobId).map(job->job.getStatus()==expected).orElse(false))return;
            Thread.sleep(100);
        }
        fail("Job did not reach "+expected+"; current status="+jobs.findById(jobId).map(Job::getStatus).orElse(null));
    }

    private void awaitKafkaConsumed(int partition,long offset)throws Exception {
        try(AdminClient admin=AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,kafka.getBootstrapServers()))){
            TopicPartition topicPartition=new TopicPartition("taskforge.jobs.execute",partition);
            long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(20);
            while(System.nanoTime()<deadline){
                OffsetAndMetadata committed=admin.listConsumerGroupOffsets("taskforge-workers").partitionsToOffsetAndMetadata().get(2,TimeUnit.SECONDS).get(topicPartition);
                if(committed!=null&&committed.offset()>offset)return;
                Thread.sleep(100);
            }
        }
        fail("Worker group did not acknowledge duplicate delivery at partition "+partition+" offset "+offset);
    }
}
