ALTER TABLE admin_audit_log
  ALTER COLUMN target_user_id DROP NOT NULL,
  ADD COLUMN resource_type VARCHAR(40),
  ADD COLUMN resource_id VARCHAR(100),
  ADD COLUMN request_id VARCHAR(64);

UPDATE admin_audit_log
SET resource_type = 'USER', resource_id = target_user_id::text
WHERE target_user_id IS NOT NULL;

CREATE INDEX ix_admin_audit_resource
  ON admin_audit_log(resource_type, resource_id, created_at);
