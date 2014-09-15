\set ON_ERROR_STOP 1

-- Create the user and the database. Must run as user postgres.

--CREATE USER ia_data PASSWORD 'ia_datarox'  NOCREATEDB NOCREATEUSER;
CREATE USER ia_data NOCREATEDB NOCREATEUSER;
CREATE DATABASE ia_data WITH OWNER = ia_data TEMPLATE template0 ENCODING = 'UNICODE';
