ALTER TABLE users
  ADD CONSTRAINT ck_users_role CHECK (role IN ('USER', 'ADMIN'));

ALTER TABLE jobs
  ADD CONSTRAINT ck_jobs_schedule_type CHECK (schedule_type IN ('IMMEDIATE', 'ONE_TIME', 'CRON')),
  ADD CONSTRAINT ck_jobs_priority CHECK (priority IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
  ADD CONSTRAINT ck_jobs_status CHECK (status IN ('CREATED', 'SCHEDULED', 'QUEUED', 'RUNNING', 'SUCCESS', 'FAILED', 'RETRYING', 'CANCELLED', 'TIMEOUT')),
  ADD CONSTRAINT ck_jobs_cron_definition CHECK (schedule_type <> 'CRON' OR (cron_expression IS NOT NULL AND btrim(cron_expression) <> '')),
  ADD CONSTRAINT ck_jobs_time_zone CHECK (btrim(time_zone) <> ''),
  ADD CONSTRAINT ck_jobs_scheduled_run_time CHECK (status NOT IN ('SCHEDULED', 'RETRYING') OR next_run_at IS NOT NULL),
  ADD CONSTRAINT ck_jobs_max_retries CHECK (max_retries BETWEEN 0 AND 20),
  ADD CONSTRAINT ck_jobs_timeout_seconds CHECK (timeout_seconds BETWEEN 1 AND 86400),
  ADD CONSTRAINT ck_jobs_attempt_count CHECK (attempt_count >= 0);

ALTER TABLE job_executions
  ADD CONSTRAINT ck_job_executions_status CHECK (status IN ('QUEUED', 'RUNNING', 'SUCCESS', 'FAILED', 'RETRYING', 'CANCELLED', 'TIMEOUT')),
  ADD CONSTRAINT ck_job_executions_attempt_number CHECK (attempt_number BETWEEN 1 AND 21);

ALTER TABLE workers
  ADD CONSTRAINT ck_workers_status CHECK (status IN ('ONLINE', 'BUSY', 'OFFLINE', 'DEAD'));

ALTER TABLE outbox_messages
  ADD CONSTRAINT ck_outbox_attempt_count CHECK (attempt_count >= 0);
