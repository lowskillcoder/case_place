import pandas as pd
import sqlite3

# Чтение данных из Excel файла
product_df = pd.read_excel('SQL.xlsx', sheet_name='product_directory')
print_df = pd.read_excel('SQL.xlsx', sheet_name='print_directory')
stocks_df = pd.read_excel('SQL.xlsx', sheet_name='stocks_directory')
orders_df = pd.read_excel('SQL.xlsx', sheet_name='orders_directory')

# Создание постоянной базы данных SQLite
conn = sqlite3.connect('store_db.sqlite')

# Создание таблиц с правильными типами данных и ограничениями
cursor = conn.cursor()

# Создание таблицы print_directory
cursor.execute('''
CREATE TABLE IF NOT EXISTS print_directory (
    print TEXT PRIMARY KEY,
    name_print_1 TEXT,
    name_print_2 TEXT
)
''')

# Создание таблицы product_directory
cursor.execute('''
CREATE TABLE IF NOT EXISTS product_directory (
    name_store TEXT,
    nomenclature TEXT PRIMARY KEY,
    print TEXT,
    barcode TEXT,
    FOREIGN KEY (print) REFERENCES print_directory(print)
)
''')

# Создание таблицы stocks_directory
cursor.execute('''
CREATE TABLE IF NOT EXISTS stocks_directory (
    date DATE,
    nomenclature TEXT,
    warehouse TEXT,
    value_stocks INTEGER,
    FOREIGN KEY (nomenclature) REFERENCES product_directory(nomenclature)
)
''')

# Создание таблицы orders_directory
cursor.execute('''
CREATE TABLE IF NOT EXISTS orders_directory (
    date DATE,
    nomenclature TEXT,
    orders_type TEXT,
    price DECIMAL(10,2),
    quantity_product INTEGER,
    FOREIGN KEY (nomenclature) REFERENCES product_directory(nomenclature)
)
''')

# Загрузка данных в таблицы
print_df.to_sql('print_directory', conn, if_exists='replace', index=False)
product_df.to_sql('product_directory', conn, if_exists='replace', index=False)
stocks_df.to_sql('stocks_directory', conn, if_exists='replace', index=False)
orders_df.to_sql('orders_directory', conn, if_exists='replace', index=False)

# Сохранение изменений и закрытие соединения
conn.commit()
conn.close()

print("База данных успешно создана: store_db.sqlite") 