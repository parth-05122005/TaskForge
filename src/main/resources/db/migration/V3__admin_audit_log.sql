CREATE TABLE admin_audit_log (
  id BIGSERIAL PRIMARY KEY,
  actor_user_id BIGINT NOT NULL REFERENCES users(id),
  target_user_id BIGINT NOT NULL REFERENCES users(id),
  action VARCHAR(80) NOT NULL,
  detail VARCHAR(500),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX ix_admin_audit_created ON admin_audit_log(created_at);
CREATE INDEX ix_admin_audit_target ON admin_audit_log(target_user_id,created_at);
