SELECT 'Upgrading Sentry store schema from 1.5.0-cdh5 to 2.0.0' AS Status from dual;
@006-SENTRY-711.oracle.sql;

UPDATE SENTRY_VERSION SET SCHEMA_VERSION='2.0.0', VERSION_COMMENT='Sentry release version 2.0.0' WHERE VER_ID=1;

SELECT 'Finished upgrading Sentry store schema from 1.5.0-cdh5 to 2.0.0' AS Status from dual;