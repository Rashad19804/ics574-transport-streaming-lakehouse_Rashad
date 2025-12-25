CREATE TABLE IF NOT EXISTS vehicle_positions_latest (
  vehicle_id TEXT PRIMARY KEY,
  mode TEXT,
  route TEXT,
  direction TEXT,
  lat DOUBLE PRECISION,
  lon DOUBLE PRECISION,
  speed DOUBLE PRECISION,
  delay_seconds INTEGER,
  event_time TIMESTAMPTZ,
  updated_at TIMESTAMPTZ DEFAULT NOW()
);
