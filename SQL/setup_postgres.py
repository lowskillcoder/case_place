import psycopg2
from psycopg2 import sql
import os

def execute_sql_file(conn, sql_file):
    """Выполняет SQL-скрипт из файла"""
    try:
        # Читаем файл как бинарный и декодируем в UTF-8
        with open(sql_file, 'rb') as f:
            content = f.read().decode('utf-8')
        
        # Разделяем команды по точкам с запятой, но сохраняем многострочные блоки
        commands = []
        current_command = []
        in_dollar_quote = False
        
        for line in content.split('\n'):
            line = line.strip()
            if not line:
                continue
                
            if '$$' in line:
                in_dollar_quote = not in_dollar_quote
                current_command.append(line)
                if not in_dollar_quote:
                    commands.append('\n'.join(current_command))
                    current_command = []
            elif in_dollar_quote:
                current_command.append(line)
            elif line.endswith(';'):
                current_command.append(line)
                commands.append('\n'.join(current_command))
                current_command = []
            else:
                current_command.append(line)
        
        if current_command:
            commands.append('\n'.join(current_command))
        
        with conn.cursor() as cur:
            # Создаем схему business в отдельной транзакции
            try:
                cur.execute("CREATE SCHEMA IF NOT EXISTS business")
                conn.commit()
                print("Схема business создана или уже существует")
            except Exception as e:
                print(f"Ошибка при создании схемы business: {e}")
                conn.rollback()
                return
            
            # Создаем таблицы в правильном порядке
            print("\nСоздание таблиц...")
            try:
                # Создаем таблицу employees первой
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS business.employees (
                        id SERIAL PRIMARY KEY,
                        name TEXT NOT NULL,
                        position TEXT,
                        salary NUMERIC(10,2)
                    )
                """)
                print("Таблица employees создана")
                
                # Создаем таблицу departments
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS business.departments (
                        id SERIAL PRIMARY KEY,
                        name TEXT NOT NULL,
                        manager_id INTEGER REFERENCES business.employees(id)
                    )
                """)
                print("Таблица departments создана")
                
                # Создаем таблицу product_directory
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS business.product_directory (
                        name_store TEXT,
                        nomenclature TEXT PRIMARY KEY,
                        print TEXT,
                        barcode TEXT
                    )
                """)
                print("Таблица product_directory создана")
                
                # Создаем таблицу ddl_log
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS business.ddl_log (
                        id SERIAL PRIMARY KEY,
                        action_type TEXT,
                        table_name TEXT,
                        sql_statement TEXT,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                print("Таблица ddl_log создана")
                
                # Создаем таблицу product_directory_changes
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS business.product_directory_changes (
                        id SERIAL PRIMARY KEY,
                        action_type TEXT,
                        old_nomenclature TEXT,
                        new_nomenclature TEXT,
                        old_print TEXT,
                        new_print TEXT,
                        old_barcode TEXT,
                        new_barcode TEXT,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                print("Таблица product_directory_changes создана")
                
                conn.commit()
            except Exception as e:
                print(f"Ошибка при создании таблиц: {e}")
                conn.rollback()
                return
            
            # Создаем пользователей
            print("\nСоздание пользователей...")
            try:
                # Создаем admin_user
                cur.execute("SELECT 1 FROM pg_roles WHERE rolname = 'admin_user'")
                if not cur.fetchone():
                    cur.execute("CREATE USER admin_user WITH PASSWORD 'admin123'")
                    print("Пользователь admin_user создан")
                
                # Создаем read_user
                cur.execute("SELECT 1 FROM pg_roles WHERE rolname = 'read_user'")
                if not cur.fetchone():
                    cur.execute("CREATE USER read_user WITH PASSWORD 'read123'")
                    print("Пользователь read_user создан")
                
                conn.commit()
            except Exception as e:
                print(f"Ошибка при создании пользователей: {e}")
                conn.rollback()
                return
            
            # Назначаем права
            print("\nНазначение прав...")
            try:
                cur.execute("GRANT ALL PRIVILEGES ON SCHEMA business TO admin_user")
                cur.execute("GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA business TO admin_user")
                cur.execute("GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA business TO admin_user")
                print("Права для admin_user назначены")
                
                cur.execute("GRANT USAGE ON SCHEMA business TO read_user")
                cur.execute("GRANT SELECT ON ALL TABLES IN SCHEMA business TO read_user")
                print("Права для read_user назначены")
                
                conn.commit()
            except Exception as e:
                print(f"Ошибка при назначении прав: {e}")
                conn.rollback()
                return
            
            # Создаем функции и триггеры
            print("\nСоздание функций и триггеров...")
            try:
                # Функция для логирования DDL действий
                cur.execute("""
                    CREATE OR REPLACE FUNCTION business.log_ddl()
                    RETURNS event_trigger
                    LANGUAGE plpgsql
                    AS $$
                    BEGIN
                        INSERT INTO business.ddl_log (action_type, table_name, sql_statement)
                        VALUES (TG_EVENT, TG_TAG, current_query());
                    END;
                    $$;
                """)
                print("Функция log_ddl создана")
                
                # Триггер для DDL действий
                cur.execute("""
                    CREATE EVENT TRIGGER log_ddl_trigger
                    ON ddl_command_end
                    EXECUTE FUNCTION business.log_ddl();
                """)
                print("Триггер log_ddl_trigger создан")
                
                # Функция для логирования изменений в product_directory
                cur.execute("""
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
                """)
                print("Функция log_product_changes создана")
                
                # Триггер для product_directory
                cur.execute("""
                    CREATE TRIGGER log_product_directory_changes
                    AFTER UPDATE ON business.product_directory
                    FOR EACH ROW
                    EXECUTE FUNCTION business.log_product_changes();
                """)
                print("Триггер log_product_directory_changes создан")
                
                conn.commit()
            except Exception as e:
                print(f"Ошибка при создании функций и триггеров: {e}")
                conn.rollback()
                return
        
        print("\nSQL-скрипт успешно выполнен")
    except Exception as e:
        print(f"Ошибка при выполнении SQL-скрипта: {e}")
        conn.rollback()

def main():
    # Параметры подключения к системной базе данных
    sys_db_params = {
        'dbname': 'postgres',
        'user': 'postgres',
        'password': 'password',
        'host': 'localhost',
        'port': 'port'
    }

    try:
        # Подключение к системной базе данных
        conn = psycopg2.connect(**sys_db_params)
        conn.autocommit = True  # Включаем autocommit для создания базы данных
        
        print("Подключение к PostgreSQL успешно установлено")
        
        # Создаем базу данных
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = 'company_db'")
            if not cur.fetchone():
                cur.execute("CREATE DATABASE company_db")
                print("База данных company_db создана")
            else:
                print("База данных company_db уже существует")
        
        # Закрываем соединение с системной базой
        conn.close()
        
        # Подключаемся к новой базе данных
        db_params = sys_db_params.copy()
        db_params['dbname'] = 'company_db'
        conn = psycopg2.connect(**db_params)
        conn.autocommit = False  # Отключаем autocommit для выполнения остальных команд
        
        print("Подключение к базе данных company_db установлено")
        
        # Выполняем остальные команды
        execute_sql_file(conn, 'setup_postgres.sql')
        
    except Exception as e:
        print(f"Ошибка: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
            print("Соединение с PostgreSQL закрыто")

if __name__ == "__main__":
    main() 