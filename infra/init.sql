-- User profiles table: stores computed  ML features per user
CREATE TABLE user_profiles(
    user_id VARCHAR PRIMARY KEY,
    purchase_frequency FLOAT,
    avg_cart_value FLOAT,
    category_affinity JSONB,
    recency_score FLOAT,
    session_depth INT,
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Offers catalog: available personalization offers
CREATE TABLE offers_catalog(
    offer_id VARCHAR PRIMARY KEY,
    name VARCHAR,
    discount_pct FLOAT,
    category VARCHAR,
    min_cart_value FLOAT,
    active BOOLEAN DEFAULT TRUE
);

-- Events Table: raw user behavior events
CREATE TABLE events(
    event_id SERIAL PRIMARY KEY,
    user_id VARCHAR,
    event_type VARCHAR,
    payload JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Seed the offers catalog with sample data
INSERT INTO offers_catalog VALUES ('OFF001', '10% Off Electronics', 10, 'electronics', 50, true), ('OFF002', 'Free Shipping', 0, 'all', 0, true), ('OFF003', '15% Off Apparel', 15, 'apparel', 30, true), ('OFF004', '$5 Off Cart', 5, 'all', 40, true);