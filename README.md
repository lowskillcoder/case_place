# Тестовое задание для компании Case Place

## 📋 Содержание
- [Python часть](#python-часть)
- [SQL часть](#sql-часть)
  - [Первая часть](#первая-часть)
  - [Вторая часть](#вторая-часть)

## Python часть

### Файлы:
1. `Данные для задания Python.xlsx` - входные данные от заказчика
2. `competitor_analysis.py` - скрипт обработки данных с подробными комментариями
3. `Задание 1.xlsx` - результат обработки входных данных

### Описание:
Скрипт `competitor_analysis.py` обрабатывает данные из Excel-файла и создает отчет в формате Excel с результатами анализа.

## SQL часть

### Первая часть

#### Файлы:
1. `SQL.xlsx` - данные из текстового задания "Тестовое задание SQL" в формате Excel
2. `create_db.py` - создает базу данных (3 пункт)
3. `store_db.sqlite` - база данных с результатами обработанных данных из SQL.xlsx 
4. `execute_query.py` - содержит SQL-запросы для каждого задания
5. `Результаты SQL запросов.xlsx` - выходной файл с результатами запросов (каждый запрос на отдельном листе)

### Вторая часть

#### Файлы:
6. `setup_postgres.sql` - SQL-скрипт для создания триггеров и базы данных
7. `setup_postgres.py` - Python-скрипт для подключения к PostgreSQL и выполнения SQL-скрипта

## 🔧 Требования
- Python 3.8+
- PostgreSQL 14+ (для второй части SQL)
- Библиотеки из requirements.txt

## 📥 Установка
```bash
pip install -r requirements.txt
```

## 🚀 Запуск
1. Python часть:
```bash
python competitor_analysis.py
```

2. SQL часть (первая):
```bash
python create_db.py
python execute_query.py
```

3. SQL часть (вторая):
```bash
python setup_postgres.py
```

# PostgreSQL Database Setup

Проект для настройки и инициализации базы данных PostgreSQL с логированием DDL действий и изменений в таблице product_directory.

## Структура проекта

```
.
├── SQL/
│   ├── setup_postgres.py    # Скрипт для создания и настройки базы данных
│   └── setup_postgres.sql   # SQL-команды для создания объектов базы данных
├── .gitignore               # Игнорируемые файлы
└── README.md                # Документация проекта
```

## Требования

- Python 3.6+
- PostgreSQL 12+
- psycopg2-binary

## Установка зависимостей

```bash
pip install psycopg2-binary
```

## Настройка базы данных

1. Убедитесь, что PostgreSQL установлен и запущен
2. Проверьте параметры подключения в `setup_postgres.py`:
   ```python
   sys_db_params = {
       'dbname': 'postgres',
       'user': 'postgres',
       'password': 'ваш_пароль',
       'host': 'localhost',
       'port': 'port'
   }
   ```
3. Запустите скрипт:
   ```bash
   python SQL/setup_postgres.py
   ```

## Создаваемые объекты

### Схема
- `business` - основная схема для хранения данных

### Таблицы
- `employees` - информация о сотрудниках
- `departments` - информация о подразделениях
- `product_directory` - справочник продукции
- `ddl_log` - логирование DDL действий
- `product_directory_changes` - логирование изменений в product_directory

### Пользователи
- `admin_user` - пользователь с полными правами
- `read_user` - пользователь с правами только на чтение

### Триггеры
- `log_ddl_trigger` - логирование DDL действий
- `log_product_directory_changes` - логирование изменений в product_directory

## Права доступа

- `admin_user` имеет полные права на схему `business`
- `read_user` имеет права только на чтение таблиц в схеме `business`

## Логирование

- Все DDL действия (CREATE, ALTER, DROP) логируются в таблицу `ddl_log`
- Все изменения в таблице `product_directory` логируются в таблицу `product_directory_changes`