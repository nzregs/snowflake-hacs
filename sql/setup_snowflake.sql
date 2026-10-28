-- =============================================================================
-- Snowflake Exporter for Home Assistant: account setup
--
-- Creates a dedicated service user, role, network policy, warehouse, database
-- and schema for the integration, and grants the role everything it needs:
-- read/write/delete on tables, plus creating tables, views, stages, pipes,
-- streams, tasks, sequences and file formats. Grants cover existing objects
-- and future objects created by any role.
--
-- Edit the variables in section 0, then run the whole script in Snowsight or
-- SnowSQL as a user who has the SECURITYADMIN and SYSADMIN roles.
--
-- After the script runs, add the integration in Home Assistant. The setup
-- form shows an ALTER USER ... SET RSA_PUBLIC_KEY statement. Run it as
-- SECURITYADMIN to finish connecting.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 0. Parameters: change these to suit your account
-- -----------------------------------------------------------------------------
SET db_name        = 'HOMEASSISTANTDB';
SET schema_name    = 'SENSORDATA';
SET warehouse_name = 'HOMEASSISTANT_WH';
SET user_name      = 'HOMEASSISTANT';
SET role_name      = 'HOMEASSISTANT_EXPORTER';
SET policy_name    = 'HOMEASSISTANT_NETWORK';

-- Public IP address(es) your Home Assistant server connects from, in CIDR
-- notation and comma separated, e.g. '198.51.100.7/32,203.0.113.0/24'.
-- Find yours at https://ifconfig.me from the Home Assistant host.
-- WARNING: '0.0.0.0/0' allows connections from anywhere. Replace it before running.
SET allowed_ips    = '0.0.0.0/0';

SET fq_schema      = $db_name || '.' || $schema_name;


-- -----------------------------------------------------------------------------
-- 1. Role and service user (key-pair auth only, no password)
-- -----------------------------------------------------------------------------
USE ROLE USERADMIN;

CREATE ROLE IF NOT EXISTS IDENTIFIER($role_name)
    COMMENT = 'Home Assistant Snowflake Exporter';

SET create_user_sql =
    'CREATE USER IF NOT EXISTS ' || $user_name ||
    ' TYPE = SERVICE' ||
    ' DEFAULT_ROLE = ' || $role_name ||
    ' DEFAULT_WAREHOUSE = ' || $warehouse_name ||
    ' DEFAULT_NAMESPACE = ''' || $fq_schema || '''' ||
    ' COMMENT = ''Home Assistant Snowflake Exporter service user''';
EXECUTE IMMEDIATE $create_user_sql;

GRANT ROLE IDENTIFIER($role_name) TO USER IDENTIFIER($user_name);

-- Let SYSADMIN manage objects created by the role
GRANT ROLE IDENTIFIER($role_name) TO ROLE SYSADMIN;


-- -----------------------------------------------------------------------------
-- 2. Network policy, attached to the user
-- -----------------------------------------------------------------------------
USE ROLE SECURITYADMIN;

SET create_policy_sql = (SELECT
    'CREATE NETWORK POLICY IF NOT EXISTS ' || $policy_name ||
    ' ALLOWED_IP_LIST = (''' || REPLACE($allowed_ips, ',', ''',''') || ''')' ||
    ' COMMENT = ''Home Assistant Snowflake Exporter''');
EXECUTE IMMEDIATE $create_policy_sql;

SET alter_user_sql =
    'ALTER USER ' || $user_name || ' SET NETWORK_POLICY = ' || $policy_name;
EXECUTE IMMEDIATE $alter_user_sql;


-- -----------------------------------------------------------------------------
-- 3. Warehouse, database and schema
-- -----------------------------------------------------------------------------
USE ROLE SYSADMIN;

CREATE WAREHOUSE IF NOT EXISTS IDENTIFIER($warehouse_name)
    WAREHOUSE_SIZE = XSMALL
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Home Assistant Snowflake Exporter';

CREATE DATABASE IF NOT EXISTS IDENTIFIER($db_name);
CREATE SCHEMA IF NOT EXISTS IDENTIFIER($fq_schema);


-- -----------------------------------------------------------------------------
-- 4. Grants on the warehouse, database and schema
-- -----------------------------------------------------------------------------
USE ROLE SECURITYADMIN;

GRANT USAGE, OPERATE, MONITOR ON WAREHOUSE IDENTIFIER($warehouse_name)
    TO ROLE IDENTIFIER($role_name);

GRANT USAGE, MONITOR ON DATABASE IDENTIFIER($db_name)
    TO ROLE IDENTIFIER($role_name);

GRANT USAGE, MONITOR,
      CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE PIPE, CREATE STREAM,
      CREATE TASK, CREATE SEQUENCE, CREATE FILE FORMAT
    ON SCHEMA IDENTIFIER($fq_schema)
    TO ROLE IDENTIFIER($role_name);


-- -----------------------------------------------------------------------------
-- 5. Grants on existing objects in the schema
-- -----------------------------------------------------------------------------
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE
    ON ALL TABLES IN SCHEMA IDENTIFIER($fq_schema) TO ROLE IDENTIFIER($role_name);
GRANT SELECT ON ALL VIEWS IN SCHEMA IDENTIFIER($fq_schema) TO ROLE IDENTIFIER($role_name);
GRANT USAGE, READ, WRITE ON ALL STAGES IN SCHEMA IDENTIFIER($fq_schema) TO ROLE IDENTIFIER($role_name);
-- Pipes don't support bulk (ALL/FUTURE) grants. The role owns any pipes it creates;
-- grant pipes made by other roles individually: GRANT OPERATE, MONITOR ON PIPE <name> TO ROLE ...
GRANT SELECT ON ALL STREAMS IN SCHEMA IDENTIFIER($fq_schema) TO ROLE IDENTIFIER($role_name);
GRANT OPERATE, MONITOR ON ALL TASKS IN SCHEMA IDENTIFIER($fq_schema) TO ROLE IDENTIFIER($role_name);
GRANT USAGE ON ALL SEQUENCES IN SCHEMA IDENTIFIER($fq_schema) TO ROLE IDENTIFIER($role_name);
GRANT USAGE ON ALL FILE FORMATS IN SCHEMA IDENTIFIER($fq_schema) TO ROLE IDENTIFIER($role_name);

-- -----------------------------------------------------------------------------
-- 6. Future grants: objects created later in the schema by any role
-- -----------------------------------------------------------------------------
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE
    ON FUTURE TABLES IN SCHEMA IDENTIFIER($fq_schema) TO ROLE IDENTIFIER($role_name);
GRANT SELECT ON FUTURE VIEWS IN SCHEMA IDENTIFIER($fq_schema) TO ROLE IDENTIFIER($role_name);
GRANT USAGE, READ, WRITE ON FUTURE STAGES IN SCHEMA IDENTIFIER($fq_schema) TO ROLE IDENTIFIER($role_name);
GRANT SELECT ON FUTURE STREAMS IN SCHEMA IDENTIFIER($fq_schema) TO ROLE IDENTIFIER($role_name);
GRANT OPERATE, MONITOR ON FUTURE TASKS IN SCHEMA IDENTIFIER($fq_schema) TO ROLE IDENTIFIER($role_name);
GRANT USAGE ON FUTURE SEQUENCES IN SCHEMA IDENTIFIER($fq_schema) TO ROLE IDENTIFIER($role_name);
GRANT USAGE ON FUTURE FILE FORMATS IN SCHEMA IDENTIFIER($fq_schema) TO ROLE IDENTIFIER($role_name);


-- -----------------------------------------------------------------------------
-- 7. Check the result
-- -----------------------------------------------------------------------------
SHOW GRANTS TO ROLE IDENTIFIER($role_name);
SHOW FUTURE GRANTS IN SCHEMA IDENTIFIER($fq_schema);
DESCRIBE USER IDENTIFIER($user_name);
