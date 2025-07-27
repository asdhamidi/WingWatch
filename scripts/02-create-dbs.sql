-- Creates required databases for Airflow and avnalytics, if they do not exist.
-- Grants all privileges on the created databases to the airflow user.
-- Check and create airflow database if not exists
SELECT 'CREATE DATABASE airflow WITH OWNER asad'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec

-- Grant privileges on airflow database
GRANT ALL PRIVILEGES ON DATABASE airflow TO asad;

-- Check and create avnalytics database if not exists
SELECT 'CREATE DATABASE avnalytics WITH OWNER asad'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'avnalytics')\gexec

-- Grant privileges on avnalytics database
GRANT ALL PRIVILEGES ON DATABASE avnalytics TO asad;