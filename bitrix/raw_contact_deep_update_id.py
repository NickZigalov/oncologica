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

# Скрипт предназначен для полной перезагрузки текущих контактов в Битрикс. 
# Сделано из-за того, что иногда заведенный контакт удаляется совсем, в том числе из системы и как по-другому синхронизировать все удаленные контакты с уже загруженными я не придумал.
# В raw_contact и update я буду выгружать небольшими партиями, а этот список, перезаливаемый каждый день будет накладываться на общую загрузку и помечать в нем удаленные контакты

def get_date_range():
    """
    Возвращает фиксированный диапазон дат
    """
    # Получаем текущую дату
    current_date = datetime.now()

    # Вычисляем дату 45 дней назад
    #start_date_obj = current_date - timedelta(days=60)

    # Задайте здесь нужные даты
    start_date = "2026-01-01"  # Дата начала в формате ГГГГ-ММ-ДД
    #end_date = "2026-02-20"    # Дата окончания в формате ГГГГ-ММ-ДД
    
    # Форматируем даты в строки ГГГГ-ММ-ДД
    #start_date = start_date_obj.strftime("%Y-%m-%d")
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
        "UF_CRM_1759990941520", #тип контакта (служебное)
        "SOURCE_ID", 
        "BIRTHDATE", 
        "DATE_CREATE",
        "DATE_MODIFY",
        "UF_CRM_1759989190637", #донор-онко дата
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
    Сохранение контактов в PostgreSQL с полной перезаливкой таблицы
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
    
    conn = None
    cursor = None
    
    try:
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        
        # Создаем схему если не существует
        cursor.execute("CREATE SCHEMA IF NOT EXISTS bitrix;")
        
        # Удаляем старую таблицу если существует
        cursor.execute("DROP TABLE IF EXISTS bitrix.raw_contact_deep_update;")
        
        # Создаем новую таблицу
        create_table_sql = """
        CREATE TABLE bitrix.raw_contact_deep_update (
            id BIGINT PRIMARY KEY,
            loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
        """
        cursor.execute(create_table_sql)
        conn.commit()
        
        # SQL для вставки данных
        insert_sql = """
        INSERT INTO bitrix.raw_contact_deep_update (
            id
        ) VALUES (%s);
        """
        
        # Вставляем данные пакетами
        batch_size = 100
        total_saved = 0
        
        for i in range(0, len(contacts_data), batch_size):
            batch = contacts_data[i:i + batch_size]
            
            for contact in batch:
                contact_data = (
                    int(contact.get('ID')),  # Преобразуем ID в int для PostgreSQL
                )
                cursor.execute(insert_sql, contact_data)
                total_saved += 1
            
            conn.commit()
            print(f"Сохранено {total_saved} из {len(contacts_data)} контактов")
        
        print(f"✅ Успешно сохранено контактов: {total_saved}")
        
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

def main():
    """
    Основная функция
    """
    print("=== ЗАПУСК СКРИПТА ДЛЯ ЗАГРУЗКИ КОНТАКТОВ ===")
    start_time = datetime.now()
    
    # Получаем фиксированный диапазон дат
    start_date, end_date = get_date_range()
    
    print("\n=== ЗАГРУЗКА КОНТАКТОВ ===")
    
    # Получаем данные из Bitrix24
    contacts_data = get_all_bitrix_contacts_by_id(start_date, end_date)
    
    if contacts_data:
        print(f"\n=== ПОЛУЧЕНО {len(contacts_data)} КОНТАКТОВ ===")
        
        # Сохраняем в базу данных с полной перезаливкой
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