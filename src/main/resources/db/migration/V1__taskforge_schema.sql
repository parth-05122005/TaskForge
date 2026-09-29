CREATE TABLE users (
  id BIGSERIAL PRIMARY KEY,
  email VARCHAR(255) NOT NULL UNIQUE,
  password_hash VARCHAR(255) NOT NULL,
  role VARCHAR(32) NOT NULL DEFAULT 'USER',
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE jobs (
  id BIGSERIAL PRIMARY KEY,
  version BIGINT NOT NULL DEFAULT 0,
  owner_id BIGINT NOT NULL REFERENCES users(id),
  name VARCHAR(160) NOT NULL,
  description VARCHAR(2000),
  type VARCHAR(80) NOT NULL,
  payload TEXT NOT NULL,
  schedule_type VARCHAR(24) NOT NULL,
  cron_expression VARCHAR(120),
  time_zone VARCHAR(80) NOT NULL DEFAULT 'UTC',
  next_run_at TIMESTAMPTZ,
  priority VARCHAR(24) NOT NULL,
  status VARCHAR(24) NOT NULL,
  max_retries INTEGER NOT NULL CHECK (max_retries >= 0),
  timeout_seconds INTEGER NOT NULL CHECK (timeout_seconds > 0),
  attempt_count INTEGER NOT NULL DEFAULT 0,
  cancellation_requested BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ix_jobs_due ON jobs(status,next_run_at);
CREATE INDEX ix_jobs_owner ON jobs(owner_id,created_at);

CREATE TABLE workers (
  id VARCHAR(100) PRIMARY KEY,
  hostname VARCHAR(160) NOT NULL,
  status VARCHAR(16) NOT NULL,
  last_heartbeat TIMESTAMPTZ NOT NULL,
  current_job_id BIGINT,
  registered_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX ix_worker_heartbeat ON workers(status,last_heartbeat);

CREATE TABLE job_executions (
  id BIGSERIAL PRIMARY KEY,
  version BIGINT NOT NULL DEFAULT 0,
  job_id BIGINT NOT NULL REFERENCES jobs(id),
  worker_id VARCHAR(100),
  attempt_number INTEGER NOT NULL CHECK (attempt_number > 0),
  lease_token VARCHAR(36),
  lease_until TIMESTAMPTZ,
  status VARCHAR(24) NOT NULL,
  started_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  duration_ms BIGINT,
  error_message VARCHAR(2000),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT uq_execution_attempt UNIQUE(job_id,attempt_number),
  CONSTRAINT fk_execution_worker FOREIGN KEY(worker_id) REFERENCES workers(id)
);
CREATE INDEX ix_execution_job ON job_executions(job_id,attempt_number);
CREATE INDEX ix_execution_lease ON job_executions(status,lease_until);

CREATE TABLE outbox_messages (
  id BIGSERIAL PRIMARY KEY,
  topic VARCHAR(200) NOT NULL,
  message_key VARCHAR(200) NOT NULL,
  payload TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at TIMESTAMPTZ,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  next_attempt_at TIMESTAMPTZ,
  last_error VARCHAR(500)
);
CREATE INDEX ix_outbox_pending ON outbox_messages(published_at,next_attempt_at,created_at);

CREATE TABLE job_events (
  id BIGSERIAL PRIMARY KEY,
  job_id BIGINT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  execution_id BIGINT REFERENCES job_executions(id),
  status VARCHAR(24) NOT NULL,
  worker_id VARCHAR(100),
  detail VARCHAR(500),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ix_job_event_created ON job_events(created_at,id);
CREATE INDEX ix_job_event_job ON job_events(job_id,created_at);
