ALTER TABLE workers
  ADD CONSTRAINT fk_worker_current_job
  FOREIGN KEY (current_job_id) REFERENCES jobs(id) ON DELETE SET NULL;

CREATE INDEX ix_jobs_priority ON jobs(priority);
CREATE INDEX ix_worker_current_job ON workers(current_job_id);
