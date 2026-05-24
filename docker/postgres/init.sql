-- Enable the vector extension for high-performance embeddings
CREATE EXTENSION IF NOT EXISTS vector;

-- Enable Apache AGE for graph database capabilities
CREATE EXTENSION IF NOT EXISTS age;

-- Ensure the 'age' module is loaded and search path includes ag_catalog
LOAD 'age';
ALTER DATABASE knoggin_db SET search_path = ag_catalog, "$user", public;

-- Initialize the knoggin graph if it doesn't already exist
SELECT create_graph('knoggin') WHERE NOT EXISTS (SELECT * FROM ag_graph WHERE name = 'knoggin');
