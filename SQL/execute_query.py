import sqlite3
import pandas as pd

# Подключение к базе данных
conn = sqlite3.connect('store_db.sqlite')

# Отладочный запрос для проверки данных
debug_query = """
SELECT 
    date,
    warehouse,
    value_stocks
FROM 
    stocks_directory
WHERE 
    warehouse = 'Склад 1'
ORDER BY 
    date
"""

# SQL запрос для задания 1
query1 = """
SELECT 
    p.name_store,
    p.nomenclature,
    o.date,
    o.price * o.quantity_product as revenue
FROM 
    product_directory p
JOIN 
    orders_directory o ON p.nomenclature = o.nomenclature
WHERE 
    p.name_store = 'Магазин 1'
ORDER BY 
    o.date
"""

# SQL запрос для задания 2
query2 = """
SELECT 
    pd.print,
    pd.name_print_1,
    pd.name_print_2
FROM 
    print_directory pd
LEFT JOIN 
    product_directory p ON pd.print = p.print
WHERE 
    p.print IS NULL
"""

# SQL запрос для задания 3
query3 = """
SELECT 
    p.nomenclature,
    p.print,
    pd.name_print_1,
    pd.name_print_2
FROM 
    product_directory p
JOIN 
    print_directory pd ON p.print = pd.print
WHERE 
    pd.name_print_1 IS NOT NULL 
    AND pd.name_print_2 IS NOT NULL
"""

# SQL запрос для задания 4
query4 = """
SELECT 
    p.name_store,
    p.nomenclature,
    s.warehouse,
    s.value_stocks
FROM 
    product_directory p
JOIN 
    stocks_directory s ON p.nomenclature = s.nomenclature
WHERE 
    s.warehouse = 'Склад 1'
    AND date(s.date) = '2024-10-18'
    AND s.value_stocks > 0
"""

# SQL запрос для задания 5
query5 = """
SELECT 
    p.barcode,
    o.date,
    COUNT(*) as order_count,
    SUM(o.price * o.quantity_product) as revenue,
    SUM(o.price * o.quantity_product * 0.95) as profit_after_tax
FROM 
    product_directory p
JOIN 
    orders_directory o ON p.nomenclature = o.nomenclature
WHERE 
    p.barcode = 'Code_1'
GROUP BY 
    p.barcode,
    o.date
ORDER BY 
    o.date
"""

# SQL запрос для задания 6
query6 = """
SELECT 
    pd.print,
    pd.name_print_1,
    SUM(o.quantity_product) as total_sales
FROM 
    print_directory pd
JOIN 
    product_directory p ON pd.print = p.print
JOIN 
    orders_directory o ON p.nomenclature = o.nomenclature
GROUP BY 
    pd.print,
    pd.name_print_1
ORDER BY 
    total_sales DESC
LIMIT 1
"""

# Выполнение запросов и сохранение результатов
try:
    # Проверяем данные в stocks_directory
    debug_result = pd.read_sql_query(debug_query, conn)
    
    # Создаем Excel writer для записи в один файл
    with pd.ExcelWriter('Результаты SQL запросов.xlsx', engine='openpyxl') as writer:
        # Задание 1
        result1 = pd.read_sql_query(query1, conn)
        result1.to_excel(writer, sheet_name='Задание 1', index=False)
        
        # Задание 2
        result2 = pd.read_sql_query(query2, conn)
        result2.to_excel(writer, sheet_name='Задание 2', index=False)
        
        # Задание 3
        result3 = pd.read_sql_query(query3, conn)
        result3.to_excel(writer, sheet_name='Задание 3', index=False)
        
        # Задание 4
        result4 = pd.read_sql_query(query4, conn)
        result4.to_excel(writer, sheet_name='Задание 4', index=False)
        
        # Задание 5
        result5 = pd.read_sql_query(query5, conn)
        result5.to_excel(writer, sheet_name='Задание 5', index=False)
        
        # Задание 6
        result6 = pd.read_sql_query(query6, conn)
        result6.to_excel(writer, sheet_name='Задание 6', index=False)
    
except Exception as e:
    print(f"Произошла ошибка: {str(e)}")
finally:
    conn.close() 