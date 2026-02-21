import requests
import psycopg2
from datetime import datetime, timedelta
import json
import time
import os
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()
url = os.getenv('URL')
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')
database = os.getenv('DB_DB')  # cred файл API GoogleSheets
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
bitrix_url=url+"crm.deal.list"

def get_date_range():
    """
    Возвращает фиксированный диапазон дат
    """
    # Получаем текущую дату
    current_date = datetime.now()

    # Вычисляем дату 45 дней назад
    start_date_obj = current_date - timedelta(days=45)

    # Задайте здесь нужные даты
    #start_date = "2026-02-01"  # Дата начала в формате ГГГГ-ММ-ДД
    #end_date = "2026-02-20"    # Дата окончания в формате ГГГГ-ММ-ДД
    
    # Форматируем даты в строки ГГГГ-ММ-ДД
    start_date = start_date_obj.strftime("%Y-%m-%d")
    end_date = current_date.strftime("%Y-%m-%d")

    # Добавляем время к датам для Bitrix24
    start_datetime = f"{start_date}T00:00:00+03:00"
    end_datetime = f"{end_date}T23:59:59+03:00"
    
    print(f"Диапазон дат: с {start_datetime} по {end_datetime}")
    
    return start_datetime, end_datetime

def get_all_bitrix_deals_by_id(start_date, end_date):
    """
    Получение всех сделок с пагинацией через фильтрацию по ID
    """    
    select_fields = [
        "ID", 
        "UF_CRM_1677762047168",  # city
        "UF_CRM_1677823106425", #city2
        "UF_CRM_1707744048", #region projivaniya
        "UF_CRM_DEAL_1690287429983",  # delivery_adress
        "UF_CRM_640598068EED2", #nozology
        "UF_CRM_1677823173593", #nozology2
        "UF_CRM_1769006393421", #nozology_last
        "UF_CRM_1708419883700" #пол 4100-male 4102-fermale
    ]
    
    base_filter = {
        ">DATE_CREATE": start_date,
        "<=DATE_CREATE": end_date
    }
    
    all_deals = []
    last_id = 0
    batch_count = 0
    max_batches = 800  # 200 * 50 = 10,000 записей максимум
    
    print(f"Начинаем загрузку данных из Bitrix24 с {start_date} по {end_date}...")
    
    while batch_count < max_batches:
        batch_count += 1
        print(f"Пакет {batch_count}, последний ID: {last_id}")
        
        # Добавляем фильтр по ID для пагинации
        current_filter = base_filter.copy()
        if last_id > 0:
            current_filter[">ID"] = str(last_id)  # Преобразуем в строку для Bitrix24
        
        payload = {
            "SELECT": select_fields,
            "FILTER": current_filter,
            "ORDER": {"ID": "ASC"},
        }
        
        try:
            response = requests.post(
                bitrix_url,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json"
                },
                data=json.dumps(payload, ensure_ascii=False),
                timeout=60
            )
            
            if response.status_code != 200:
                print(f"Ошибка HTTP {response.status_code}")
                break
                
            result = response.json()
            
            if 'error' in result:
                print(f"Ошибка Bitrix24: {result['error']}")
                break
                
            if 'result' not in result:
                print("Нет поля 'result' в ответе")
                break
                
            deals_batch = result['result']
            
            if not deals_batch:
                print("Пустой результат - завершение")
                break
            
            # Сохраняем последний ID для следующего запроса (преобразуем в int)
            last_id = int(deals_batch[-1].get('ID', 0))
            
            all_deals.extend(deals_batch)
            print(f"Получено {len(deals_batch)} сделок (всего: {len(all_deals)})")
            
            # Если получено меньше 50 записей, значит это последний пакет
            if len(deals_batch) < 50:
                print("Получено меньше 50 записей - завершение")
                break
            
            # Пауза между запросами
            time.sleep(0.3)
            
        except Exception as e:
            print(f"Ошибка: {e}")
            break
    
    return all_deals

# def get_bitrix_deals_simple(start_date, end_date):
#     """
#     Простой метод - один запрос на все данные
#     """
    
#     select_fields = [
#         "ID", 
#         "UF_CRM_1677762047168",  # city
#         "UF_CRM_1677823106425", #city2
#         "UF_CRM_1707744048", #region projivaniya
#         "UF_CRM_DEAL_1690287429983",  # delivery_adress
#         "UF_CRM_640598068EED2", #nozology
#         "UF_CRM_1677823173593", #nozology2
#         "UF_CRM_1769006393421", #nozology_last
#         "UF_CRM_1708419883700" #пол 4100-male 4102-fermale
#     ]
    
#     filter_data = {
#         ">DATE_CREATE": start_date,
#         "<=DATE_CREATE": end_date
#     }
    
#     print(f"Пытаемся получить все данные одним запросом с {start_date} по {end_date}...")
    
#     # Пробуем запросить больше данных
#     payload = {
#         "SELECT": select_fields,
#         "FILTER": filter_data,
#         "ORDER": {"ID": "ASC"},
#         "START": 0
#     }
    
#     try:
#         response = requests.post(
#             bitrix_url,
#             headers={
#                 "Content-Type": "application/json",
#                 "Accept": "application/json"
#             },
#             data=json.dumps(payload, ensure_ascii=False),
#             timeout=60
#         )
        
#         if response.status_code != 200:
#             print(f"Ошибка HTTP {response.status_code}")
#             return []
            
#         result = response.json()
        
#         if 'error' in result:
#             print(f"Ошибка Bitrix24: {result['error']}")
#             return []
            
#         if 'result' not in result:
#             print("Нет поля 'result' в ответе")
#             return []
            
#         deals = result['result']
#         print(f"Получено {len(deals)} сделок")
        
#         return deals
        
#     except Exception as e:
#         print(f"Ошибка: {e}")
#         return []

def save_deals_to_postgres(deals_data, start_date, end_date):
    """
    Сохранение сделок в PostgreSQL с использованием UPSERT
    """
    if not deals_data:
        print("Нет данных для сохранения")
        return False
        
    print(f"Начинаем сохранение {len(deals_data)} сделок в PostgreSQL...")
    
    conn_params = {
        'host': host,
        'port': port,
        'database': database,
        'user': db_user,
        'password': db_password
    }
    
    try:
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        
        # Создаем схему и таблицу
        cursor.execute("CREATE SCHEMA IF NOT EXISTS bitrix;")
        
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS bitrix.raw_deals_short (
            id BIGINT PRIMARY KEY,
            city TEXT NULL,
            city2 TEXT NULL,
            region_projivaniya TEXT NULL,
            delivery_address TEXT NULL,
            nozology TEXT NULL,
            nozology2 TEXT NULL,
            loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            nozology_last TEXT NULL,
            sex TEXT NULL
        );
        """
        cursor.execute(create_table_sql)
        conn.commit()
        
        # SQL для UPSERT с новыми названиями полей
        upsert_sql = """
        INSERT INTO bitrix.raw_deals_short (
            id, city, city2, region_projivaniya, delivery_address, nozology, nozology2, nozology_last, sex
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            city = EXCLUDED.city,
            city2 = EXCLUDED.city2,
            region_projivaniya = EXCLUDED.region_projivaniya,
            delivery_address = EXCLUDED.delivery_address,
            nozology = EXCLUDED.nozology,
            nozology2 = EXCLUDED.nozology2,
            loaded_at = CURRENT_TIMESTAMP,
            nozology_last = EXCLUDED.nozology_last,
            sex = EXCLUDED.sex;
        """
        
        # Преобразуем даты для сохранения в БД
        start_date_db = datetime.fromisoformat(start_date.replace('T', ' ').split('+')[0])
        end_date_db = datetime.fromisoformat(end_date.replace('T', ' ').split('+')[0])
        
        # Вставляем данные пакетами
        batch_size = 100
        total_saved = 0
        total_updated = 0
        
        for i in range(0, len(deals_data), batch_size):
            batch = deals_data[i:i + batch_size]
            
            for deal in batch:
                deal_data = (
                    int(deal.get('ID')),  # Преобразуем ID в int для PostgreSQL
                    deal.get('UF_CRM_1677762047168'),  # city
                    deal.get('UF_CRM_1677823106425'), #city2
                    deal.get('UF_CRM_1707744048'), #region_projivaniya
                    deal.get('UF_CRM_DEAL_1690287429983'),  # delivery_adress
                    deal.get('UF_CRM_640598068EED2'), #nozology
                    deal.get('UF_CRM_1677823173593'), #nozology2
                    deal.get('UF_CRM_1769006393421'), #nozology_last
                    deal.get('UF_CRM_1708419883700'), #sex
                )
                cursor.execute(upsert_sql, deal_data)
                
                # Проверяем, была ли вставка или обновление
                if cursor.statusmessage.startswith('INSERT'):
                    total_saved += 1
                else:
                    total_updated += 1
            
            conn.commit()
            print(f"Обработано пакет из {len(batch)} сделок (новых: {total_saved}, обновлено: {total_updated})")
        
        print(f"✅ Успешно обработано сделок: {total_saved + total_updated}")
        print(f"   - Новых записей: {total_saved}")
        print(f"   - Обновленных записей: {total_updated}")
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

def test_api_limits(start_date, end_date):
    """
    Тестируем ограничения API
    """
    print("=== ТЕСТИРОВАНИЕ API ===")
    
    # Тест 1: Простой запрос
    payload1 = {
        "SELECT": ["ID", "TITLE"],
        "FILTER": {
            ">DATE_CREATE": start_date,
            "<=DATE_CREATE": end_date
        },
        "ORDER": {"ID": "ASC"},
        "START": 0
    }
    
    response1 = requests.post(bitrix_url, headers={"Content-Type": "application/json"}, data=json.dumps(payload1))
    result1 = response1.json()
    print(f"Тест 1 - START=0: {len(result1.get('result', []))} записей")
    
    # Тест 2: Запрос со START=50
    payload2 = {
        "SELECT": ["ID", "TITLE"],
        "FILTER": {
            ">DATE_CREATE": start_date,
            "<=DATE_CREATE": end_date
        },
        "ORDER": {"ID": "ASC"},
        "START": 50
    }
    
    response2 = requests.post(bitrix_url, headers={"Content-Type": "application/json"}, data=json.dumps(payload2))
    result2 = response2.json()
    print(f"Тест 2 - START=50: {len(result2.get('result', []))} записей")
    
    # Сравним ID
    if result1.get('result') and result2.get('result'):
        ids1 = [deal['ID'] for deal in result1['result']]
        ids2 = [deal['ID'] for deal in result2['result']]
        print(f"ID из первого запроса: {ids1[:5]}...")
        print(f"ID из второго запроса: {ids2[:5]}...")
        print(f"Пересекаются ли ID: {bool(set(ids1) & set(ids2))}")

def main():
    """
    Основная функция
    """
    print("=== ЗАПУСК СКРИПТА ===")
    start_time = datetime.now()
    
    # Получаем фиксированный диапазон дат
    start_date, end_date = get_date_range()
    
    # Сначала протестируем API
    test_api_limits(start_date, end_date)
    
    print("\n=== ЗАГРУЗКА ДАННЫХ ===")
    
    # Пробуем разные методы
    print("Метод 1: Пагинация по ID")
    deals_data = get_all_bitrix_deals_by_id(start_date, end_date)
    
    # if not deals_data or len(deals_data) <= 50:
    #     print("Метод 1 не сработал, пробуем Метод 2: Простой запрос")
    #     deals_data = get_bitrix_deals_simple(start_date, end_date)
    
    if deals_data:
        print(f"\n=== ПОЛУЧЕНО {len(deals_data)} СДЕЛОК ===")
        
        # Сохраняем в базу данных
        success = save_deals_to_postgres(deals_data, start_date, end_date)
        
        if success:
            print("✅ Данные успешно сохранены в PostgreSQL")
        else:
            print("❌ Ошибка при сохранении данных")
    else:
        print("❌ Не удалось получить данные из Bitrix24")
    
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"\n=== ЗАВЕРШЕНО ===")
    print(f"Общее время выполнения: {duration}")

if __name__ == "__main__":
    main()