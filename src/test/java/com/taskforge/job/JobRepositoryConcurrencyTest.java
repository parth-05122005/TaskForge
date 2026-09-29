package com.taskforge.job;

import com.taskforge.auth.*;
import com.taskforge.worker.*;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import com.taskforge.common.TaskForgeMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.*;
import java.time.Instant;
import java.util.concurrent.*;
import org.mockito.Mockito;
import org.springframework.data.redis.core.StringRedisTemplate;
import static org.junit.jupiter.api.Assertions.*;

@DataJpaTest
@AutoConfigureTestDatabase(replace=AutoConfigureTestDatabase.Replace.NONE)
@Testcontainers(disabledWithoutDocker=true)
class JobRepositoryConcurrencyTest {
    @Container static final PostgreSQLContainer<?> postgres=new PostgreSQLContainer<>("postgres:17-alpine");
    @DynamicPropertySource static void database(DynamicPropertyRegistry r){r.add("spring.datasource.url",postgres::getJdbcUrl);r.add("spring.datasource.username",postgres::getUsername);r.add("spring.datasource.password",postgres::getPassword);}
    @Autowired JobRepository jobs;@Autowired UserRepository users;@Autowired JobExecutionRepository executions;@Autowired WorkerRepository workers;@Autowired OutboxRepository outbox;@Autowired JobEventRepository events;@Autowired PlatformTransactionManager transactionManager;@Autowired JdbcTemplate jdbc;

    @BeforeEach void clearCommittedFixtures(){TransactionTemplate cleanup=new TransactionTemplate(transactionManager);cleanup.setPropagationBehavior(org.springframework.transaction.TransactionDefinition.PROPAGATION_REQUIRES_NEW);cleanup.execute(s->{events.deleteAll();executions.deleteAll();workers.deleteAll();jobs.deleteAll();outbox.deleteAll();users.deleteAll();return null;});}

    @Test void retentionDeletesOnlyAcknowledgedRowsPastCutoffInBoundedBatches(){
        Instant cutoff=Instant.now().minusSeconds(60);
        OutboxMessage oldPublished=new OutboxMessage("topic","old","{}");oldPublished.markPublished();oldPublished=outbox.saveAndFlush(oldPublished);
        OutboxMessage recentPublished=new OutboxMessage("topic","recent","{}");recentPublished.markPublished();recentPublished=outbox.saveAndFlush(recentPublished);
        OutboxMessage pending=new OutboxMessage("topic","pending","{}");pending=outbox.saveAndFlush(pending);
        jdbc.update("update outbox_messages set published_at=? where id=?",java.sql.Timestamp.from(cutoff.minusSeconds(1)),oldPublished.getId());

        assertEquals(1,outbox.deletePublishedBefore(cutoff,1_000));
        assertFalse(outbox.existsById(oldPublished.getId()));
        assertTrue(outbox.existsById(recentPublished.getId()));
        assertTrue(outbox.existsById(pending.getId()));
    }

    @Test void deleteLockQueryLocksExecutionHistoryInStableOrder(){
        User owner=users.save(new User("delete-lock-order@example.com","hash"));
        Job job=jobs.save(new Job(owner,"delete lock order",null,"REPORT","{}",ScheduleType.IMMEDIATE,null,Instant.now(),JobPriority.MEDIUM,1,30));
        JobExecution first=executions.saveAndFlush(new JobExecution(job,1));
        JobExecution second=executions.saveAndFlush(new JobExecution(job,2));

        var locked=executions.lockAllByJobId(job.getId());

        assertEquals(java.util.List.of(first.getId(),second.getId()),locked.stream().map(JobExecution::getId).toList());
    }

    @Test @org.springframework.transaction.annotation.Transactional(propagation=org.springframework.transaction.annotation.Propagation.NOT_SUPPORTED)
    void concurrentSchedulersDoNotClaimTheSameDueJob() throws Exception {
        User owner=users.save(new User("concurrency@example.com","hash"));
        Job job=jobs.save(new Job(owner,"race",null,"REPORT","{}",ScheduleType.ONE_TIME,null,Instant.now(),JobPriority.HIGH,2,30));
        TransactionTemplate tx=new TransactionTemplate(transactionManager);CountDownLatch locked=new CountDownLatch(1),release=new CountDownLatch(1);ExecutorService pool=Executors.newFixedThreadPool(2);
        try{
            Future<Integer> first=pool.submit(()->tx.execute(s->{int count=jobs.lockDue(Instant.now(),50).size();locked.countDown();try{release.await(5,TimeUnit.SECONDS);}catch(InterruptedException e){Thread.currentThread().interrupt();}return count;}));
            assertTrue(locked.await(5,TimeUnit.SECONDS));
            int second=tx.execute(s->jobs.lockDue(Instant.now(),50).size());
            release.countDown();
            assertEquals(1,first.get(5,TimeUnit.SECONDS));assertEquals(0,second);
        }finally{release.countDown();pool.shutdownNow();}
    }

    @Test @org.springframework.transaction.annotation.Transactional(propagation=org.springframework.transaction.annotation.Propagation.NOT_SUPPORTED)
    void twoSchedulerInstancesDispatchOneJobExactlyOnce() throws Exception {
        User owner=users.save(new User("two-schedulers@example.com","hash"));
        Job job=jobs.save(new Job(owner,"scheduler race",null,"REPORT","{}",ScheduleType.ONE_TIME,null,Instant.now(),JobPriority.HIGH,2,30));
        var mapper=new com.fasterxml.jackson.databind.ObjectMapper().findAndRegisterModules();
        Scheduler firstScheduler=new Scheduler(jobs,executions,outbox,new JobEventService(events,outbox,mapper),mapper);
        Scheduler secondScheduler=new Scheduler(jobs,executions,outbox,new JobEventService(events,outbox,mapper),mapper);
        TransactionTemplate tx=new TransactionTemplate(transactionManager);
        CountDownLatch start=new CountDownLatch(1);
        ExecutorService pool=Executors.newFixedThreadPool(2);
        try {
            Future<?> first=pool.submit(()->{await(start);tx.execute(s->{firstScheduler.dispatchDue();return null;});});
            Future<?> second=pool.submit(()->{await(start);tx.execute(s->{secondScheduler.dispatchDue();return null;});});
            start.countDown();
            first.get(10,TimeUnit.SECONDS);second.get(10,TimeUnit.SECONDS);
            assertEquals(JobStatus.QUEUED,jobs.findById(job.getId()).orElseThrow().getStatus());
            assertEquals(1,executions.findByJobIdOrderByRunNumberDescAttemptNumberDesc(job.getId()).size());
            assertEquals(2,outbox.count()); // execute message plus persisted QUEUED lifecycle event
        } finally {pool.shutdownNow();}
    }

    @Test @org.springframework.transaction.annotation.Transactional(propagation=org.springframework.transaction.annotation.Propagation.NOT_SUPPORTED)
    void concurrentOutboxPublishersNeverOwnTheSamePendingRow() throws Exception {
        outbox.save(new OutboxMessage("taskforge.jobs.execute","job-outbox-lock","{}"));
        TransactionTemplate tx=new TransactionTemplate(transactionManager);
        CountDownLatch locked=new CountDownLatch(1),release=new CountDownLatch(1);
        ExecutorService pool=Executors.newSingleThreadExecutor();
        try {
            Future<Integer> first=pool.submit(()->tx.execute(s->{int count=outbox.lockPending(50).size();locked.countDown();await(release);return count;}));
            assertTrue(locked.await(5,TimeUnit.SECONDS));
            int second=tx.execute(s->outbox.lockPending(50).size());
            release.countDown();
            assertEquals(1,first.get(5,TimeUnit.SECONDS));
            assertEquals(0,second);
        } finally {release.countDown();pool.shutdownNow();}
    }

    @Test @org.springframework.transaction.annotation.Transactional(propagation=org.springframework.transaction.annotation.Propagation.NOT_SUPPORTED)
    void twoWorkersRacingDuplicateDeliveryProduceOnlyOneDurableClaim() throws Exception {
        User owner=users.save(new User("two-workers@example.com","hash"));
        Worker firstWorker=workers.save(new Worker("claim-worker-1","host-1"));
        Worker secondWorker=workers.save(new Worker("claim-worker-2","host-2"));
        Job job=new Job(owner,"worker race",null,"REPORT","{}",ScheduleType.IMMEDIATE,null,Instant.now(),JobPriority.MEDIUM,2,30);
        job.beginScheduledRun();job.transition(JobStatus.QUEUED);job.nextAttempt();job=jobs.save(job);
        JobExecution execution=executions.save(new JobExecution(job,1));
        Long jobId=job.getId(),executionId=execution.getId();
        var mapper=new com.fasterxml.jackson.databind.ObjectMapper().findAndRegisterModules();
        JobEventService eventService=new JobEventService(events,outbox,mapper);
        var metrics=new TaskForgeMetrics(new SimpleMeterRegistry(),jobs,workers);
        var firstService=new com.taskforge.worker.ExecutionWorkflowService(jobs,executions,workers,eventService,outbox,mapper,metrics);
        var secondService=new com.taskforge.worker.ExecutionWorkflowService(jobs,executions,workers,eventService,outbox,mapper,metrics);
        JobMessage message=new JobMessage(jobId,executionId,1,"REPORT","{}","MEDIUM",1,30);
        TransactionTemplate tx=new TransactionTemplate(transactionManager);
        CountDownLatch start=new CountDownLatch(1);
        ExecutorService pool=Executors.newFixedThreadPool(2);
        try {
            Future<String> first=pool.submit(()->{await(start);return tx.execute(s->firstService.claim(message,firstWorker.getId()));});
            Future<String> second=pool.submit(()->{await(start);return tx.execute(s->secondService.claim(message,secondWorker.getId()));});
            start.countDown();
            String firstToken=first.get(10,TimeUnit.SECONDS),secondToken=second.get(10,TimeUnit.SECONDS);
            assertNotEquals(firstToken==null,secondToken==null);
            JobExecution claimed=executions.findById(executionId).orElseThrow();
            assertEquals(JobStatus.RUNNING,claimed.getStatus());
            assertEquals(JobStatus.RUNNING,jobs.findById(jobId).orElseThrow().getStatus());
            assertTrue(claimed.getWorkerId().equals(firstWorker.getId())||claimed.getWorkerId().equals(secondWorker.getId()));
        } finally {pool.shutdownNow();}
    }

    @Test @org.springframework.transaction.annotation.Transactional(propagation=org.springframework.transaction.annotation.Propagation.NOT_SUPPORTED)
    void crashedWorkerLeaseIsRecoveredAndJobIsMadeRetryable() {
        User owner=users.save(new User("crash-recovery@example.com","hash"));
        Worker worker=workers.save(new Worker("crashed-worker","crashed-host"));
        Job job=new Job(owner,"recover me",null,"REPORT","{}",ScheduleType.IMMEDIATE,null,Instant.now(),JobPriority.MEDIUM,2,30);
        job.beginScheduledRun();job.transition(JobStatus.QUEUED);job.nextAttempt();job.transition(JobStatus.RUNNING);job=jobs.save(job);
        JobExecution execution=executions.save(new JobExecution(job,1));
        Long jobId=job.getId(),executionId=execution.getId();
        TransactionTemplate tx=new TransactionTemplate(transactionManager);
        tx.execute(s->{assertEquals(1,executions.claimQueued(executionId,jobId,1,worker.getId(),"expired-lease",-60));return null;});
        var mapper=new com.fasterxml.jackson.databind.ObjectMapper().findAndRegisterModules();
        JobEventService eventService=new JobEventService(events,outbox,mapper);
        var recovery=new com.taskforge.worker.LeaseRecovery(executions,jobs,workers,eventService,Mockito.mock(StringRedisTemplate.class),outbox,mapper,new TaskForgeMetrics(new SimpleMeterRegistry(),jobs,workers));
        tx.execute(s->{recovery.recoverExpiredLeases();return null;});
        assertEquals(JobStatus.RETRYING,jobs.findById(jobId).orElseThrow().getStatus());
        assertEquals(JobStatus.FAILED,executions.findById(executionId).orElseThrow().getStatus());
        assertEquals(WorkerStatus.ONLINE,workers.findById(worker.getId()).orElseThrow().getStatus());
        assertEquals(1,outbox.count()); // recovered lifecycle event; retryable crash is not dead-lettered
    }

    @Test @org.springframework.transaction.annotation.Transactional(propagation=org.springframework.transaction.annotation.Propagation.NOT_SUPPORTED)
    void recoveredExhaustedAttemptProducesDeadLetterAndHonorsCancellation() {
        User owner=users.save(new User("cancel-recovery@example.com","hash"));
        Worker worker=workers.save(new Worker("cancelled-worker","cancelled-host"));
        Worker exhaustedWorker=workers.save(new Worker("exhausted-worker","exhausted-host"));
        Job job=new Job(owner,"cancel while crashed",null,"REPORT","{}",ScheduleType.IMMEDIATE,null,Instant.now(),JobPriority.MEDIUM,0,30);
        job.beginScheduledRun();job.transition(JobStatus.QUEUED);job.nextAttempt();job.transition(JobStatus.RUNNING);job.requestCancellation();job=jobs.save(job);
        JobExecution execution=executions.save(new JobExecution(job,1));
        Long jobId=job.getId(),executionId=execution.getId();
        Job exhaustedJob=new Job(owner,"exhausted crashed attempt",null,"REPORT","{}",ScheduleType.IMMEDIATE,null,Instant.now(),JobPriority.MEDIUM,0,30);
        exhaustedJob.beginScheduledRun();exhaustedJob.transition(JobStatus.QUEUED);exhaustedJob.nextAttempt();exhaustedJob.transition(JobStatus.RUNNING);exhaustedJob=jobs.save(exhaustedJob);
        JobExecution exhaustedExecution=executions.save(new JobExecution(exhaustedJob,1));
        Long exhaustedJobId=exhaustedJob.getId(),exhaustedExecutionId=exhaustedExecution.getId();
        TransactionTemplate tx=new TransactionTemplate(transactionManager);
        tx.execute(s->{assertEquals(1,executions.claimQueued(executionId,jobId,1,worker.getId(),"expired-cancel-lease",-60));return null;});
        tx.execute(s->{assertEquals(1,executions.claimQueued(exhaustedExecutionId,exhaustedJobId,1,exhaustedWorker.getId(),"expired-exhausted-lease",-60));return null;});
        var mapper=new com.fasterxml.jackson.databind.ObjectMapper().findAndRegisterModules();
        JobEventService eventService=new JobEventService(events,outbox,mapper);
        var recovery=new com.taskforge.worker.LeaseRecovery(executions,jobs,workers,eventService,Mockito.mock(StringRedisTemplate.class),outbox,mapper,new TaskForgeMetrics(new SimpleMeterRegistry(),jobs,workers));
        tx.execute(s->{recovery.recoverExpiredLeases();return null;});
        assertEquals(JobStatus.CANCELLED,jobs.findById(jobId).orElseThrow().getStatus());
        assertEquals(JobStatus.FAILED,jobs.findById(exhaustedJobId).orElseThrow().getStatus());
        assertEquals(3,outbox.count()); // two lifecycle events and one DLQ for the exhausted crash
    }

    private static void await(CountDownLatch latch) {
        try { latch.await(); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException(e); }
    }

    @Test @org.springframework.transaction.annotation.Transactional(propagation=org.springframework.transaction.annotation.Propagation.NOT_SUPPORTED)
    void queuedExecutionCanOnlyBeClaimedOnce(){
        User owner=users.save(new User("claim@example.com","hash"));Worker worker=workers.save(new Worker("worker-test","test-host"));
        Job job=new Job(owner,"claim",null,"REPORT","{}",ScheduleType.IMMEDIATE,null,Instant.now(),JobPriority.MEDIUM,1,1);job.beginScheduledRun();job.transition(JobStatus.QUEUED);job=jobs.save(job);
        JobExecution execution=executions.save(new JobExecution(job,1));Long jobId=job.getId(),executionId=execution.getId();
        var mapper=new com.fasterxml.jackson.databind.ObjectMapper().findAndRegisterModules();var eventService=new JobEventService(events,outbox,mapper);
        var workflow=new com.taskforge.worker.ExecutionWorkflowService(jobs,executions,workers,eventService,outbox,mapper,new TaskForgeMetrics(new SimpleMeterRegistry(),jobs,workers));
        JobMessage message=new JobMessage(jobId,executionId,job.getRunNumber(),job.getType(),job.getPayload(),job.getPriority().name(),1,job.getTimeoutSeconds());TransactionTemplate tx=new TransactionTemplate(transactionManager);
        String claimed=tx.execute(s->workflow.claim(message,worker.getId()));
        String duplicate=tx.execute(s->workflow.claim(message,worker.getId()));
        assertNotNull(claimed);assertNull(duplicate);
        JobExecution initiallyClaimed=executions.findById(executionId).orElseThrow();
        assertFalse(initiallyClaimed.getLeaseUntil().isAfter(initiallyClaimed.getStartedAt().plusSeconds(21)),"The initial lease must honor timeout plus the 20-second safety ceiling");
        tx.execute(s->executions.extendWorkerLeases(worker.getId(),Instant.now().plusSeconds(30)));
        JobExecution active=executions.findById(executionId).orElseThrow();
        assertFalse(active.getLeaseUntil().isAfter(active.getStartedAt().plusSeconds(50)),"A live heartbeat must not keep a non-cooperative handler leased forever");
    }

    @Test @org.springframework.transaction.annotation.Transactional(propagation=org.springframework.transaction.annotation.Propagation.NOT_SUPPORTED)
    void schedulerCommitsQueuedStateExecutionAndOutboxTogether(){
        User owner=users.save(new User("outbox@example.com","hash"));
        Job job=jobs.save(new Job(owner,"outbox",null,"REPORT","{}",ScheduleType.ONE_TIME,null,Instant.now(),JobPriority.HIGH,1,30));
        var mapper=new com.fasterxml.jackson.databind.ObjectMapper().findAndRegisterModules();Scheduler scheduler=new Scheduler(jobs,executions,outbox,new JobEventService(events,outbox,mapper),mapper);
        new TransactionTemplate(transactionManager).execute(status->{scheduler.dispatchDue();return null;});
        assertEquals(JobStatus.QUEUED,jobs.findById(job.getId()).orElseThrow().getStatus());
        assertEquals(1,executions.findByJobIdOrderByRunNumberDescAttemptNumberDesc(job.getId()).size());
        assertEquals(2,outbox.count());
        assertFalse(events.findTop200ByJobIdOrderByCreatedAtDesc(job.getId()).isEmpty());
    }
}
