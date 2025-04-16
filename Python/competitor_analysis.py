# Ответ на задание 0: вероятность 1/9

import pandas as pd
import numpy as np

# Чтение данных из Excel файла
def read_excel_data():
    # Чтение списка номенклатур с листа "Задание1"
    nomenclature_df = pd.read_excel('Данные для задания Python.xlsx', sheet_name='Задание1')
    
    # Чтение отчетов по продажам с соответствующих листов
    # Используем header=[0,1] для чтения многоуровневых заголовков
    ip1_df = pd.read_excel('Данные для задания Python.xlsx', 
                          sheet_name='Отчет о продажах ИП Иванов', 
                          header=[0,1])
    ip2_df = pd.read_excel('Данные для задания Python.xlsx', 
                          sheet_name='Отчет о продажах ИП Петров', 
                          header=[0,1])
    ip3_df = pd.read_excel('Данные для задания Python.xlsx', 
                          sheet_name='Отчет о продажах ИП Сидоров', 
                          header=[0,1])
    
    # Находим правильное название столбца номенклатуры
    def get_nomenclature_column(df):
        for col in df.columns:
            if isinstance(col, tuple) and 'Номенклатура' in col[1]:
                return col
        return None
    
    # Получаем правильные названия столбцов для каждого DataFrame
    ip1_nomenclature_col = get_nomenclature_column(ip1_df)
    ip2_nomenclature_col = get_nomenclature_column(ip2_df)
    ip3_nomenclature_col = get_nomenclature_column(ip3_df)
    
    return nomenclature_df, ip1_df, ip2_df, ip3_df, ip1_nomenclature_col, ip2_nomenclature_col, ip3_nomenclature_col

# Функция для задания 1: Определение принадлежности номенклатуры к ИП
def determine_ip_ownership(nomenclature_df, ip1_df, ip2_df, ip3_df, ip1_nomenclature_col, ip2_nomenclature_col, ip3_nomenclature_col):
    # Создаем копию DataFrame для результатов
    result_df = nomenclature_df.copy()
    
    # Преобразуем столбец ИП в строковый тип
    result_df['ИП'] = result_df['ИП'].astype(str)
    
    # Проверяем принадлежность к каждому ИП
    for index, row in result_df.iterrows():
        nomenclature = row['Номенклатура']
        
        # Проверяем наличие в отчетах ИП
        if ip1_nomenclature_col and nomenclature in ip1_df[ip1_nomenclature_col].values:
            result_df.at[index, 'ИП'] = 'Иванов'
        elif ip2_nomenclature_col and nomenclature in ip2_df[ip2_nomenclature_col].values:
            result_df.at[index, 'ИП'] = 'Петров'
        elif ip3_nomenclature_col and nomenclature in ip3_df[ip3_nomenclature_col].values:
            result_df.at[index, 'ИП'] = 'Сидоров'
        else:
            result_df.at[index, 'ИП'] = '-'
    
    return result_df

# Функция для задания 2: Подсчет количества заказов
def calculate_orders(nomenclature_df, ip1_df, ip2_df, ip3_df, ip1_nomenclature_col, ip2_nomenclature_col, ip3_nomenclature_col):
    # Создаем словарь для хранения количества заказов
    orders_dict = {}
    
    # Инициализируем счетчик для всех номенклатур
    for nomenclature in nomenclature_df['Номенклатура']:
        orders_dict[nomenclature] = 0
    
    # Функция для подсчета заказов в одном DataFrame
    def count_orders_in_df(df, nomenclature_col):
        if nomenclature_col and ('Заказано', 'шт') in df.columns:
            # Создаем список столбцов для группировки и суммирования
            group_cols = [nomenclature_col]
            sum_col = [('Заказано', 'шт')]
            
            # Группируем по номенклатуре и суммируем заказы
            orders = df.groupby(group_cols)[sum_col].sum()
            
            # Обновляем общий словарь
            for idx in orders.index:
                # Проверяем тип индекса
                if isinstance(idx, tuple):
                    nomenclature = idx[0]  # Получаем номенклатуру из кортежа
                else:
                    nomenclature = idx  # Используем индекс напрямую
                
                count = orders.loc[idx, sum_col[0]]  # Получаем количество заказов
                if nomenclature in orders_dict:
                    orders_dict[nomenclature] += count
    
    # Подсчитываем заказы для каждого ИП
    count_orders_in_df(ip1_df, ip1_nomenclature_col)
    count_orders_in_df(ip2_df, ip2_nomenclature_col)
    count_orders_in_df(ip3_df, ip3_nomenclature_col)
    
    return orders_dict

# Функция для задания 3: Расчет выручки
def calculate_revenue(nomenclature_df, ip1_df, ip2_df, ip3_df, ip1_nomenclature_col, ip2_nomenclature_col, ip3_nomenclature_col):
    # Создаем словарь для хранения выручки
    revenue_dict = {}
    
    # Инициализируем счетчик для всех номенклатур
    for nomenclature in nomenclature_df['Номенклатура']:
        revenue_dict[nomenclature] = 0
    
    # Функция для подсчета выручки в одном DataFrame
    def count_revenue_in_df(df, nomenclature_col):
        if nomenclature_col and ('Заказано', 'себестоимость') in df.columns:
            # Создаем список столбцов для группировки и суммирования
            group_cols = [nomenclature_col]
            sum_col = [('Заказано', 'себестоимость')]
            
            # Группируем по номенклатуре и суммируем себестоимость
            revenue = df.groupby(group_cols)[sum_col].sum()
            
            # Обновляем общий словарь
            for idx in revenue.index:
                # Проверяем тип индекса
                if isinstance(idx, tuple):
                    nomenclature = idx[0]  # Получаем номенклатуру из кортежа
                else:
                    nomenclature = idx  # Используем индекс напрямую
                
                cost = revenue.loc[idx, sum_col[0]]  # Получаем себестоимость
                # Рассчитываем выручку: себестоимость / 0.83 (так как себестоимость = 83% от выручки)
                total_revenue = cost / 0.83
                if nomenclature in revenue_dict:
                    revenue_dict[nomenclature] += total_revenue
    
    # Подсчитываем выручку для каждого ИП
    count_revenue_in_df(ip1_df, ip1_nomenclature_col)
    count_revenue_in_df(ip2_df, ip2_nomenclature_col)
    count_revenue_in_df(ip3_df, ip3_nomenclature_col)
    
    return revenue_dict

# Функция для задания 4: Расчет прибыли
def calculate_profit(nomenclature_df, result_df):
    # Читаем справочник и таблицу себестоимости
    reference_df = pd.read_excel('Данные для задания Python.xlsx', sheet_name='Справочник')
    cost_df = pd.read_excel('Данные для задания Python.xlsx', sheet_name='Себестоимость')
    
    # Создаем словарь для хранения прибыли
    profit_dict = {}
    
    # Находим минимальные фиксированные затраты
    min_fixed_cost = cost_df['Фиксированные затраты, руб./шт.'].min()
    
    # Создаем словарь для хранения фиксированных затрат по категориям
    fixed_costs = dict(zip(cost_df['Категория'], cost_df['Фиксированные затраты, руб./шт.']))
    
    # Создаем словарь для хранения налоговых ставок по ИП
    tax_rates = {
        'Иванов': 0.01,
        'Петров': 0.03,
        'Сидоров': 0.05
    }
    
    # Комиссия WB
    commission_rate = 0.17
    
    # Рассчитываем прибыль для каждой номенклатуры
    for index, row in result_df.iterrows():
        nomenclature = row['Номенклатура']
        ip = row['ИП']
        orders = row['Заказы, шт.']
        revenue = row['Выручка, руб.']
        
        # Находим категорию номенклатуры
        category = reference_df[reference_df['Номенклатура'] == nomenclature]['Категория'].iloc[0]
        
        # Получаем фиксированные затраты для категории
        fixed_cost_per_unit = fixed_costs.get(category, min_fixed_cost)
        
        # Рассчитываем общие фиксированные затраты
        total_fixed_costs = fixed_cost_per_unit * orders
        
        # Рассчитываем налог
        tax_rate = tax_rates.get(ip, 0)  # Если ИП не найден, налог = 0
        tax = revenue * tax_rate
        
        # Рассчитываем комиссию
        commission = revenue * commission_rate
        
        # Рассчитываем общие затраты
        total_costs = total_fixed_costs + tax + commission
        
        # Рассчитываем прибыль
        profit = revenue - total_costs
        
        profit_dict[nomenclature] = profit
    
    return profit_dict

# Функция для задания 5: Расчет рентабельности продаж
def calculate_profitability(result_df):
    # Создаем словарь для хранения рентабельности
    profitability_dict = {}
    
    # Рассчитываем рентабельность для каждой номенклатуры
    for index, row in result_df.iterrows():
        nomenclature = row['Номенклатура']
        profit = row['Прибыль, руб.']
        revenue = row['Выручка, руб.']
        
        # Рассчитываем рентабельность в процентах
        # Проверяем, чтобы избежать деления на ноль
        if revenue != 0:
            profitability = (profit / revenue) * 100
        else:
            profitability = 0
        
        profitability_dict[nomenclature] = profitability
    
    return profitability_dict

# Функция для создания сводной таблицы
def create_pivot_table(result_df):
    # Фильтруем строки, где ИП не равно "-"
    filtered_df = result_df[result_df['ИП'] != '-']
    
    # Создаем сводную таблицу
    pivot_df = filtered_df.pivot_table(
        index='ИП',
        values=['Заказы, шт.', 'Выручка, руб.', 'Прибыль, руб.'],
        aggfunc='sum'
    )
    
    # Переименовываем столбцы для лучшей читаемости
    pivot_df.columns = ['Сумма заказов, шт.', 'Сумма выручки, руб.', 'Сумма прибыли, руб.']
    
    return pivot_df

def main():
    try:
        # Читаем данные
        nomenclature_df, ip1_df, ip2_df, ip3_df, ip1_nomenclature_col, ip2_nomenclature_col, ip3_nomenclature_col = read_excel_data()
        
        # Задание 1: Определение принадлежности номенклатуры к ИП
        result_df = determine_ip_ownership(nomenclature_df, ip1_df, ip2_df, ip3_df, 
                                         ip1_nomenclature_col, ip2_nomenclature_col, ip3_nomenclature_col)
        
        # Задание 2: Подсчет количества заказов
        orders_dict = calculate_orders(nomenclature_df, ip1_df, ip2_df, ip3_df, 
                                     ip1_nomenclature_col, ip2_nomenclature_col, ip3_nomenclature_col)
        
        # Заполняем столбец "Заказы, шт." в result_df
        result_df['Заказы, шт.'] = result_df['Номенклатура'].map(orders_dict)
        
        # Задание 3: Расчет выручки
        revenue_dict = calculate_revenue(nomenclature_df, ip1_df, ip2_df, ip3_df,
                                      ip1_nomenclature_col, ip2_nomenclature_col, ip3_nomenclature_col)
        
        # Заполняем столбец "Выручка, руб." в result_df
        result_df['Выручка, руб.'] = result_df['Номенклатура'].map(revenue_dict)
        
        # Задание 4: Расчет прибыли
        profit_dict = calculate_profit(nomenclature_df, result_df)
        
        # Заполняем столбец "Прибыль, руб." в result_df
        result_df['Прибыль, руб.'] = result_df['Номенклатура'].map(profit_dict)
        
        # Задание 5: Расчет рентабельности
        profitability_dict = calculate_profitability(result_df)
        
        # Заполняем столбец "Рентабельность, %" в result_df
        result_df['Рентабельность, %'] = result_df['Номенклатура'].map(profitability_dict)
        
        # Создаем сводную таблицу
        pivot_df = create_pivot_table(result_df)
        
        # Сохраняем результаты в один файл на разные листы
        with pd.ExcelWriter('Задание 1.xlsx') as writer:
            result_df.to_excel(writer, index=False, sheet_name='Таблица')
            pivot_df.to_excel(writer, sheet_name='Сводная таблица')
        
        
    except Exception as e:
        print(f"Произошла ошибка: {str(e)}")

def check_excel_structure():
    # Чтение всех листов для проверки структуры
    df = pd.read_excel('Данные для задания Python.xlsx', sheet_name='Задание1')
    
    df = pd.read_excel('Данные для задания Python.xlsx', sheet_name='Отчет о продажах ИП Иванов')
    
    df = pd.read_excel('Данные для задания Python.xlsx', sheet_name='Отчет о продажах ИП Петров')
    
    df = pd.read_excel('Данные для задания Python.xlsx', sheet_name='Отчет о продажах ИП Сидоров')

if __name__ == "__main__":
    main()
    check_excel_structure() 