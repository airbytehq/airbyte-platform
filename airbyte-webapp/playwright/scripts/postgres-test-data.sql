-- Create test databases
-- Note: Database creation must be done outside this script as it requires connecting to different databases

-- Users table
DROP TABLE IF EXISTS users CASCADE;
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO users (name, email) VALUES 
    ('Alice Johnson', 'alice@example.com'),
    ('Bob Smith', 'bob@example.com'),
    ('Charlie Brown', 'charlie@example.com'),
    ('Diana Prince', 'diana@example.com'),
    ('Eve Adams', 'eve@example.com');

-- Cities table
DROP TABLE IF EXISTS cities CASCADE;
CREATE TABLE cities (
    id SERIAL PRIMARY KEY,
    city VARCHAR(100),
    city_code VARCHAR(10),
    country VARCHAR(100),
    state VARCHAR(100)
);
INSERT INTO cities (city, city_code, country, state) VALUES 
    ('New York', 'NYC', 'USA', 'New York'),
    ('Los Angeles', 'LAX', 'USA', 'California'),
    ('London', 'LON', 'UK', 'England'),
    ('Tokyo', 'TYO', 'Japan', 'Tokyo'),
    ('Sydney', 'SYD', 'Australia', 'New South Wales');

-- Cars table
DROP TABLE IF EXISTS cars CASCADE;
CREATE TABLE cars (
    id SERIAL PRIMARY KEY,
    mark VARCHAR(50),
    model VARCHAR(50),
    color VARCHAR(30)
);
INSERT INTO cars (mark, model, color) VALUES 
    ('Toyota', 'Camry', 'Silver'),
    ('Honda', 'Civic', 'Blue'),
    ('Ford', 'Focus', 'Red'),
    ('Tesla', 'Model 3', 'White'),
    ('BMW', 'X5', 'Black');
