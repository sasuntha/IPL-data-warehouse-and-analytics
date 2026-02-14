CREATE SCHEMA IF NOT EXISTS ipl_analytics;
SET search_path TO ipl_analytics;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
-- Add comment AFTER schema is created
COMMENT ON SCHEMA ipl_analytics IS 'IPL Cricket Analytics Data Warehouse';