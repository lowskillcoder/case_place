-- Создание базы данных
CREATE DATABASE company_db;

-- Создание схемы business
CREATE SCHEMA business;

-- Создание таблицы для логирования DDL действий
CREATE TABLE business.ddl_log (
    id SERIAL PRIMARY KEY,
    action_type TEXT,
    table_name TEXT,
    sql_statement TEXT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Создание таблицы для логирования изменений в product_directory
CREATE TABLE business.product_directory_changes (
    id SERIAL PRIMARY KEY,
    action_type TEXT,
    old_nomenclature TEXT,
    new_nomenclature TEXT,
    old_print TEXT,
    new_print TEXT,
    old_barcode TEXT,
    new_barcode TEXT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Создание таблицы product_directory
CREATE TABLE business.product_directory (
    name_store TEXT,
    nomenclature TEXT PRIMARY KEY,
    print TEXT,
    barcode TEXT
);

-- Создание таблиц в схеме business
CREATE TABLE business.employees (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    position TEXT,
    salary NUMERIC(10,2)
);

CREATE TABLE business.departments (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    manager_id INTEGER REFERENCES business.employees(id)
);

-- Создание пользователей
CREATE USER admin_user WITH PASSWORD 'admin123';
CREATE USER read_user WITH PASSWORD 'read123';

-- Назначение прав доступа
GRANT ALL PRIVILEGES ON SCHEMA business TO admin_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA business TO admin_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA business TO admin_user;

GRANT USAGE ON SCHEMA business TO read_user;
GRANT SELECT ON ALL TABLES IN SCHEMA business TO read_user;

-- Создание функции для логирования DDL действий
CREATE OR REPLACE FUNCTION business.log_ddl()
RETURNS event_trigger
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO business.ddl_log (action_type, table_name, sql_statement)
    VALUES (TG_EVENT, TG_TAG, current_query());
END;
$$;

-- Создание триггера для DDL действий
CREATE EVENT TRIGGER log_ddl_trigger
ON ddl_command_end
EXECUTE FUNCTION business.log_ddl();

-- Создание триггера для логирования изменений в product_directory
CREATE OR REPLACE FUNCTION business.log_product_changes()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO business.product_directory_changes (
        action_type,
        old_nomenclature,
        new_nomenclature,
        old_print,
        new_print,
        old_barcode,
        new_barcode
    )
    VALUES (
        TG_OP,
        OLD.nomenclature,
        NEW.nomenclature,
        OLD.print,
        NEW.print,
        OLD.barcode,
        NEW.barcode
    );
    RETURN NEW;
END;
$$;

CREATE TRIGGER log_product_directory_changes
AFTER UPDATE ON business.product_directory
FOR EACH ROW
EXECUTE FUNCTION business.log_product_changes(); 