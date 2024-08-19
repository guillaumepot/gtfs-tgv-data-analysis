-- init.sql
DO
$$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_database WHERE datname = 'train_delay_db') THEN
      PERFORM dblink_exec('dbname=' || current_database(), 'CREATE DATABASE train_delay_db');
   END IF;
END
$$;

