-- init.sql
DO
$$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_database WHERE datname = 'db1') THEN
      PERFORM dblink_exec('dbname=' || current_database(), 'CREATE DATABASE db1');
   END IF;
END
$$;

DO
$$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_database WHERE datname = 'db2') THEN
      PERFORM dblink_exec('dbname=' || current_database(), 'CREATE DATABASE db2');
   END IF;
END
$$;