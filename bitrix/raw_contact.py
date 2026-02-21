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
bitrix_url=url+"crm.contact.list"

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

def get_all_bitrix_contacts_by_id(start_date, end_date):
    """
    Получение всех контактов с пагинацией через фильтрацию по ID
    """
    
    select_fields = [
        "ID", 
        "TYPE_ID", 
        "SOURCE_ID", 
        "BIRTHDATE", 
        "DATE_CREATE",
        "UF_CRM_CONTACT_1753438180801"  #пол
    ]
    
    base_filter = {
        ">DATE_CREATE": start_date,
        "<=DATE_CREATE": end_date
    }
    
    all_contacts = []
    last_id = 0
    batch_count = 0
    max_batches = 1000  # Максимальное количество пакетов
    
    print(f"Начинаем загрузку контактов из Bitrix24 с {start_date} по {end_date}...")
    
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
                
            contacts_batch = result['result']
            
            if not contacts_batch:
                print("Пустой результат - завершение")
                break
            
            # Сохраняем последний ID для следующего запроса (преобразуем в int)
            last_id = int(contacts_batch[-1].get('ID', 0))
            
            all_contacts.extend(contacts_batch)
            print(f"Получено {len(contacts_batch)} контактов (всего: {len(all_contacts)})")
            
            # Если получено меньше 50 записей, значит это последний пакет
            if len(contacts_batch) < 50:
                print("Получено меньше 50 записей - завершение")
                break
            
            # Пауза между запросами
            time.sleep(0.3)
            
        except Exception as e:
            print(f"Ошибка: {e}")
            break
    
    return all_contacts

def save_contacts_to_postgres(contacts_data, start_date, end_date):
    """
    Сохранение контактов в PostgreSQL с использованием UPSERT
    """
    if not contacts_data:
        print("Нет данных для сохранения")
        return False
        
    print(f"Начинаем сохранение {len(contacts_data)} контактов в PostgreSQL...")
    
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
        CREATE TABLE IF NOT EXISTS bitrix.raw_contact (
            id BIGINT PRIMARY KEY,
            type_id TEXT NULL,
            source_id TEXT NULL,
            birthdate TEXT NULL,
            date_create TIMESTAMP NULL,
            loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            sex TEXT NULL
        );
        """
        cursor.execute(create_table_sql)
        conn.commit()
        
        # SQL для UPSERT
        upsert_sql = """
        INSERT INTO bitrix.raw_contact (
            id, type_id, source_id, birthdate, date_create, sex
        ) VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            type_id = EXCLUDED.type_id,
            source_id = EXCLUDED.source_id,
            birthdate = EXCLUDED.birthdate,
            date_create = EXCLUDED.date_create,
            loaded_at = CURRENT_TIMESTAMP,
            sex = EXCLUDED.sex;
        """
        
        # Вставляем данные пакетами
        batch_size = 100
        total_saved = 0
        total_updated = 0
        
        for i in range(0, len(contacts_data), batch_size):
            batch = contacts_data[i:i + batch_size]
            
            for contact in batch:
                # Обрабатываем даты
                birthdate = contact.get('BIRTHDATE')
                date_create = contact.get('DATE_CREATE')
                
                # Преобразуем birthdate в формат даты PostgreSQL
                if birthdate:
                    try:
                        # Bitrix24 может возвращать разные форматы дат
                        if 'T' in birthdate:
                            birthdate = birthdate.split('T')[0]
                    except:
                        birthdate = None
                
                contact_data = (
                    int(contact.get('ID')),  # Преобразуем ID в int для PostgreSQL
                    contact.get('TYPE_ID'),
                    contact.get('SOURCE_ID'),
                    birthdate,
                    date_create,
                    contact.get('UF_CRM_CONTACT_1753438180801') #пол
                )
                cursor.execute(upsert_sql, contact_data)
                
                # Проверяем, была ли вставка или обновление
                if cursor.statusmessage.startswith('INSERT'):
                    total_saved += 1
                else:
                    total_updated += 1
            
            conn.commit()
            print(f"Обработано пакет из {len(batch)} контактов (новых: {total_saved}, обновлено: {total_updated})")
        
        print(f"✅ Успешно обработано контактов: {total_saved + total_updated}")
        print(f"   - Новых записей: {total_saved}")
        print(f"   - Обновленных записей: {total_updated}")
        
        # Получаем общую статистику
        cursor.execute("SELECT COUNT(*) FROM bitrix.raw_contact")
        total_count = cursor.fetchone()[0]
        print(f"📊 Всего контактов в таблице: {total_count}")
        
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
    Тестируем ограничения API для контактов
    """
    print("=== ТЕСТИРОВАНИЕ API КОНТАКТОВ ===")
        
    # Тест 1: Простой запрос
    payload1 = {
        "SELECT": ["ID", "TYPE_ID"],
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
        "SELECT": ["ID", "TYPE_ID"],
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
        ids1 = [contact['ID'] for contact in result1['result']]
        ids2 = [contact['ID'] for contact in result2['result']]
        print(f"ID из первого запроса: {ids1[:5]}...")
        print(f"ID из второго запроса: {ids2[:5]}...")
        print(f"Пересекаются ли ID: {bool(set(ids1) & set(ids2))}")

def main():
    """
    Основная функция
    """
    print("=== ЗАПУСК СКРИПТА ДЛЯ ЗАГРУЗКИ КОНТАКТОВ ===")
    start_time = datetime.now()
    
    # Получаем фиксированный диапазон дат
    start_date, end_date = get_date_range()
    
    # Сначала протестируем API
    test_api_limits(start_date, end_date)
    
    print("\n=== ЗАГРУЗКА КОНТАКТОВ ===")
    
    # Пробуем разные методы
    print("Метод 1: Пагинация по ID")
    contacts_data = get_all_bitrix_contacts_by_id(start_date, end_date)
    
    # if not contacts_data or len(contacts_data) <= 50:
    #     print("Метод 1 не сработал, пробуем Метод 2: Простой запрос")
    #     contacts_data = get_bitrix_contacts_simple(start_date, end_date)
    
    if contacts_data:
        print(f"\n=== ПОЛУЧЕНО {len(contacts_data)} КОНТАКТОВ ===")
        
        # Сохраняем в базу данных
        success = save_contacts_to_postgres(contacts_data, start_date, end_date)
        
        if success:
            print("✅ Контакты успешно сохранены в PostgreSQL")
        else:
            print("❌ Ошибка при сохранении контактов")
    else:
        print("❌ Не удалось получить контакты из Bitrix24")
    
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"\n=== ЗАВЕРШЕНО ===")
    print(f"Общее время выполнения: {duration}")

if __name__ == "__main__":
    main()