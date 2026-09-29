ALTER TABLE jobs
  ADD COLUMN run_number INTEGER NOT NULL DEFAULT 0 CHECK (run_number >= 0);

ALTER TABLE job_executions
  ADD COLUMN run_number INTEGER NOT NULL DEFAULT 0 CHECK (run_number >= 0);

ALTER TABLE job_executions
  DROP CONSTRAINT uq_execution_attempt;

ALTER TABLE job_executions
  ADD CONSTRAINT uq_execution_attempt UNIQUE (job_id, run_number, attempt_number);

CREATE INDEX ix_execution_job_run_attempt
  ON job_executions(job_id, run_number, attempt_number);
