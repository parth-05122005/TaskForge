CREATE INDEX ix_outbox_published_retention
  ON outbox_messages(published_at, id)
  WHERE published_at IS NOT NULL;
