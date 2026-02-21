# import requests
# import psycopg2
# import json
# import os
# from dotenv import load_dotenv

# # Загружаем переменные окружения
# load_dotenv()
# bitrix_url = os.getenv('BITRIX_URL')
# host = os.getenv('DB_HOST')
# port = os.getenv('DB_PORT')
# database = os.getenv('DB_DB')  # cred файл API GoogleSheets
# db_user = os.getenv('DB_USER')
# db_password = os.getenv('DB_PASSWORD')

# # Конфигурация
# DB_CONFIG = {
#         'host': host,
#         'port': port,
#         'database': database,
#         'user': db_user,
#         'password': db_password
# }

# def simple_category_sync():
#     try:
#         print("Получение категорий из Bitrix24...")
        
#         # Запрос к Bitrix API
#         payload = {"entityTypeId": 2}
#         response = requests.post(bitrix_url, json=payload)
#         data = response.json()
        
#         if 'error' in data:
#             print(f"Ошибка API: {data['error_description']}")
#             return
        
#         categories = data.get('result', {}).get('categories', [])
#         print(f"Получено {len(categories)} категорий")
        
#         # Подключение к PostgreSQL
#         conn = psycopg2.connect(**DB_CONFIG)
#         cursor = conn.cursor()
        
#         # Создание таблицы
#         cursor.execute("""
#             CREATE TABLE IF NOT EXISTS bitrix.raw_category (
#                 id INTEGER PRIMARY KEY,
#                 name VARCHAR(100) NOT NULL,
#                 sort INTEGER,
#                 entity_type_id INTEGER,
#                 is_default BOOLEAN DEFAULT FALSE,
#                 created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
#             )
#         """)
        
#         # UPSERT данных
#         for category in categories:
#             cursor.execute("""
#                 INSERT INTO bitrix.raw_category (id, name, sort, entity_type_id, is_default)
#                 VALUES (%s, %s, %s, %s, %s)
#                 ON CONFLICT (id) DO UPDATE SET
#                     name = EXCLUDED.name,
#                     sort = EXCLUDED.sort, 
#                     entity_type_id = EXCLUDED.entity_type_id,
#                     is_default = EXCLUDED.is_default
#             """, (
#                 category.get('id'),
#                 category.get('name'),
#                 category.get('sort'),
#                 category.get('entityTypeId', 2),
#                 category.get('isDefault', False)
#             ))
        
#         conn.commit()
#         conn.close()
        
#         print("Категории успешно сохранены в PostgreSQL!")
        
#     except Exception as e:
#         print(f"Ошибка: {e}")

# if __name__ == "__main__":
#     simple_category_sync()



import requests
import psycopg2
import json
import os
import time
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()
url = os.getenv('URL')
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')
database = os.getenv('DB_DB')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
bitrix_url=url+"crm.category.list"

# Конфигурация
db_config = {
    'host': host,
    'port': port,
    'database': database,
    'user': db_user,
    'password': db_password
}

def get_all_categories_with_pagination():
    """
    Получение всех категорий с пагинацией по ID
    """
    print("Начинаем загрузку категорий из Bitrix24...")
    
    all_categories = []
    last_id = 0
    batch_count = 0
    max_batches = 100
    
    while batch_count < max_batches:
        batch_count += 1
        print(f"Пакет {batch_count}, последний ID: {last_id}")
        
        # Формируем фильтр для пагинации
        filter_params = {}
        if last_id > 0:
            filter_params[">ID"] = last_id
        
        payload = {
            "entityTypeId": 2,
            "filter": filter_params,
            "order": {"ID": "ASC"}
        }
        
        try:
            response = requests.post(
                bitrix_url,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json"
                },
                json=payload,
                timeout=60
            )
            
            if response.status_code != 200:
                print(f"Ошибка HTTP {response.status_code}")
                break
                
            result = response.json()
            
            if 'error' in result:
                print(f"Ошибка Bitrix24: {result.get('error_description', result['error'])}")
                break
            
            # Получаем категории из ответа
            categories_batch = result.get('result', {}).get('categories', [])
            
            if not categories_batch:
                print("Пустой результат - завершение")
                break
            
            # Сохраняем последний ID для следующего запроса
            last_id = categories_batch[-1].get('id', 0)
            
            all_categories.extend(categories_batch)
            print(f"Получено {len(categories_batch)} категорий (всего: {len(all_categories)})")
            
            # Если получено меньше ожидаемого количества, значит это последний пакет
            if len(categories_batch) < 50:  # Предполагаем, что максимум 50 записей на запрос
                print("Получено меньше 50 записей - завершение")
                break
            
            # Пауза между запросами
            time.sleep(0.3)
            
        except Exception as e:
            print(f"Ошибка при запросе: {e}")
            break
    
    return all_categories

def save_categories_to_postgres(categories_data):
    """
    Сохранение категорий в PostgreSQL с использованием UPSERT
    """
    if not categories_data:
        print("Нет данных для сохранения")
        return False
    
    print(f"Начинаем сохранение {len(categories_data)} категорий в PostgreSQL...")
    
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # Создаем схему, если не существует
        cursor.execute("CREATE SCHEMA IF NOT EXISTS bitrix;")
        
        # Создаем таблицу
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS bitrix.raw_category (
            id INTEGER PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            sort INTEGER,
            entity_type_id INTEGER,
            is_default BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
        """
        cursor.execute(create_table_sql)
        conn.commit()
        
        # SQL для UPSERT
        upsert_sql = """
        INSERT INTO bitrix.raw_category (id, name, sort, entity_type_id, is_default)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            sort = EXCLUDED.sort,
            entity_type_id = EXCLUDED.entity_type_id,
            is_default = EXCLUDED.is_default,
            created_at = CURRENT_TIMESTAMP;
        """
        
        # Вставляем данные пакетами
        batch_size = 50
        total_saved = 0
        total_updated = 0
        
        for i in range(0, len(categories_data), batch_size):
            batch = categories_data[i:i + batch_size]
            
            for category in batch:
                category_data = (
                    category.get('id'),
                    category.get('name'),
                    category.get('sort'),
                    category.get('entityTypeId', 2),
                    category.get('isDefault', False)
                )
                cursor.execute(upsert_sql, category_data)
                
                # Проверяем, была ли вставка или обновление
                if cursor.statusmessage.startswith('INSERT'):
                    total_saved += 1
                else:
                    total_updated += 1
            
            conn.commit()
            print(f"Обработано пакет из {len(batch)} категорий (новых: {total_saved}, обновлено: {total_updated})")
        
        print(f"✅ Успешно обработано категорий: {total_saved + total_updated}")
        print(f"   - Новых записей: {total_saved}")
        print(f"   - Обновленных записей: {total_updated}")
        
        # Показываем пример сохраненных данных
        cursor.execute("SELECT id, name, is_default FROM bitrix.raw_category ORDER BY id LIMIT 5;")
        sample = cursor.fetchall()
        if sample:
            print("\nПример сохраненных данных (первые 5 записей):")
            for row in sample:
                print(f"  ID: {row[0]}, Название: {row[1]}, По умолчанию: {row[2]}")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка при работе с PostgreSQL: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def test_api_connection():
    """
    Тестируем подключение к API
    """
    print("=== ТЕСТИРОВАНИЕ ПОДКЛЮЧЕНИЯ К API ===")
    
    try:
        # Простой тестовый запрос
        payload = {
            "entityTypeId": 2,
            "order": {"ID": "ASC"}
        }
        
        response = requests.post(
            bitrix_url,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json"
            },
            json=payload,
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            if 'error' not in result:
                categories = result.get('result', {}).get('categories', [])
                print(f"✅ Подключение к API успешно")
                print(f"   Получено категорий в тестовом запросе: {len(categories)}")
                if categories:
                    print(f"   Пример: ID={categories[0].get('id')}, Название={categories[0].get('name')}")
                return True
            else:
                print(f"❌ Ошибка API: {result.get('error_description', result['error'])}")
        else:
            print(f"❌ Ошибка HTTP: {response.status_code}")
            
    except Exception as e:
        print(f"❌ Ошибка подключения: {e}")
    
    return False

def main():
    """
    Основная функция синхронизации категорий
    """
    print("=== ЗАПУСК СИНХРОНИЗАЦИИ КАТЕГОРИЙ ===")
    start_time = datetime.now()
    
    # Тестируем подключение
    if not test_api_connection():
        print("❌ Тест подключения не пройден. Прерывание выполнения.")
        return
    
    print("\n=== ЗАГРУЗКА ДАННЫХ ===")
    
    # Получаем все категории с пагинацией
    categories_data = get_all_categories_with_pagination()
    
    if categories_data:
        print(f"\n=== ПОЛУЧЕНО {len(categories_data)} КАТЕГОРИЙ ===")
        
        # Сохраняем в базу данных
        success = save_categories_to_postgres(categories_data)
        
        if success:
            print("\n✅ Данные успешно сохранены в PostgreSQL")
        else:
            print("\n❌ Ошибка при сохранении данных")
    else:
        print("\n❌ Не удалось получить данные из Bitrix24")
    
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"\n=== ЗАВЕРШЕНО ===")
    print(f"Общее время выполнения: {duration}")

if __name__ == "__main__":
    from datetime import datetime
    main()