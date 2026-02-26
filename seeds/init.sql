-- Create users table
-- Create users table
CREATE TABLE IF NOT EXISTS users (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    operation_type VARCHAR(10) NOT NULL DEFAULT 'INSERT'
);

-- Index for fast incremental queries
CREATE INDEX IF NOT EXISTS idx_users_updated_at
ON users(updated_at);

CREATE OR REPLACE FUNCTION track_user_changes()
RETURNS TRIGGER AS $$
BEGIN

IF TG_OP = 'INSERT' THEN
    NEW.operation_type = 'INSERT';
    RETURN NEW;

ELSIF TG_OP = 'UPDATE' THEN
    NEW.operation_type = 'UPDATE';
    NEW.updated_at = NOW();
    RETURN NEW;

END IF;

RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- Create watermarks table
CREATE TABLE IF NOT EXISTS watermarks (
    id SERIAL PRIMARY KEY,
    consumer_id VARCHAR(255) UNIQUE NOT NULL,
    last_exported_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
-- Insert 100,000 users only if table is empty
INSERT INTO users (name, email, created_at, updated_at, is_deleted, operation_type)
SELECT 
    'User ' || gs,
    'user' || gs || '@example.com',
    NOW() - (RANDOM() * INTERVAL '7 days'),
    NOW() - (RANDOM() * INTERVAL '7 days'),
    CASE 
        WHEN gs % 100 = 0 THEN TRUE
        ELSE FALSE
    END,
    'INSERT'
FROM generate_series(1, 100000) gs
WHERE NOT EXISTS (SELECT 1 FROM users LIMIT 1);

CREATE TRIGGER user_change_trigger
BEFORE INSERT OR UPDATE
ON users
FOR EACH ROW
EXECUTE FUNCTION track_user_changes();