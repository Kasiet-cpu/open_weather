# Open Weather Data Pipeline

Этот проект представляет собой ETL-пайплайн для сбора, хранения и анализа исторических погодных данных. Проект построен на базе **Apache Airflow**, **MinIO (S3)** и **PostgreSQL**, с использованием **DuckDB** для эффективной обработки данных.

## 🚀 Описание проекта

Пайплайн выполняет следующие задачи:
1.  **Сбор данных**: Ежедневная выгрузка исторических погодных данных с [Open-Meteo API](https://open-meteo.com/) для заданных координат.
2.  **Хранение (Data Lake)**: Сохранение "сырых" данных в формате Parquet в S3-хранилище (MinIO).
3.  **Загрузка (DWH)**: Перенос данных из S3 в PostgreSQL (слой ODS) для дальнейшего анализа.

## 🛠 Технологический стек

*   **Apache Airflow (v2.10.5)**: Оркестрация процессов (Celery Executor).
*   **Docker & Docker Compose**: Контейнеризация и запуск сервисов.
*   **MinIO**: S3-совместимое объектное хранилище.
*   **PostgreSQL**: Основное хранилище данных (DWH) и метаданных Airflow.
*   **DuckDB**: Используется внутри DAG'ов для быстрой обработки и трансфера данных (JSON -> Parquet -> Postgres).
*   **Jupyter Notebook**: Среда для аналитики и экспериментов.

## 📂 Структура проекта

*   `dags/` - Python скрипты с описанием пайплайнов (DAGs).
    *   `raw_from_open-meteo_to_s3.py` - Загрузка из API в S3.
    *   `raw_from_s3_to_postgres.py` - Загрузка из S3 в PostgreSQL.
*   `docker-compose.yaml` - Конфигурация для запуска всей инфраструктуры.
*   `notebooks/` - Папка для Jupyter ноутбуков.

## ⚙️ Установка и запуск

### Предварительные требования
*   Docker
*   Docker Compose

### Запуск

1.  Клонируйте репозиторий:
    ```bash
    git clone <your-repo-url>
    cd open-weather
    ```

2.  Запустите сервисы:
    ```bash
    docker-compose up -d
    ```

3.  Инициализация Airflow (если запускаете впервые, `airflow-init` отработает автоматически).

### Доступ к сервисам

| Сервис | Адрес | Логин / Пароль (по умолчанию) |
| :--- | :--- | :--- |
| **Airflow Webserver** | [http://localhost:8081](http://localhost:8081) | `airflow` / `airflow` |
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) | `minioadmin` / `minioadmin` |
| **Jupyter Notebook** | [http://localhost:8888](http://localhost:8888) | Token: `agro_secret` |
| **Flower (Celery)** | [http://localhost:5555](http://localhost:5555) | - |
| **Postgres DWH** | `localhost:5432` | `postgres` / `postgres` (DB: `postgres`) |

## 🔧 Настройка Airflow (Variables)

Для корректной работы DAG'ов необходимо создать следующие **Variables** в интерфейсе Airflow (Admin -> Variables):

| Key | Description | Пример значения |
| :--- | :--- | :--- |
| `bucket_name` | Имя бакета в MinIO | `agro-data` |
| `access_key` | Логин MinIO | `minioadmin` |
| `secret_key` | Пароль MinIO | `minioadmin` |
| `pg_password` | Пароль от базы DWH | `postgres` |

> **Важно**: Перед запуском DAG `raw_from_open-meteo_to_s3`, убедитесь, что бакет (например, `agro-data`) создан в MinIO.

## 📊 Описание DAGs

### 1. `raw_from_open-meteo_to_s3`
*   **Расписание**: `0 5 * * *` (Каждый день в 05:00 UTC+5).
*   **Действие**:
    *   Запрашивает погоду (температура, влажность, осадки, почвенные показатели и др.) с Open-Meteo.
    *   Сохраняет JSON во временный файл.
    *   С помощью DuckDB конвертирует в Parquet и загружает в S3.

### 2. `raw_from_s3_to_postgres`
*   **Расписание**: `0 5 * * *`.
*   **Действие**:
    *   Ожидает появление файла в S3 (`S3KeySensor`).
    *   Скачивает Parquet и загружает данные в таблицу `ods.fct_weather_history` в PostgreSQL.
    *   Создает таблицу автоматически при необходимости (через DDL, если настроено, или предполагается, что схема `ods` и таблица существуют).

## 📝 Заметки

*   Координаты сбора данных: `Use code logic` (51.70 N, 71.00 E - район Астаны).
*   Для изменения списка метрик отредактируйте список `hourly` в `raw_from_open-meteo_to_s3.py`.
