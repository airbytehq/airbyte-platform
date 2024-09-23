-- Default is 100. Give this slightly more to accommodate the multiple setup applications running at the start.
ALTER SYSTEM
SET
max_connections = 150;
