import requests
import psycopg2
import json
import logging
from typing import Dict, List, Optional
from datetime import datetime
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
bitrix_url=url+"user.get.json"

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_bitrix_employees_paginated() -> Optional[List[Dict]]:
    """
    Получает сотрудников из Bitrix24 API с пагинацией
    """
    
    # Поля для выборки
    select_fields = [
        "ID", "XML_ID", "ACTIVE", "NAME", "LAST_NAME", "SECOND_NAME", "EMAIL",
        "LAST_LOGIN", "DATE_REGISTER", "PERSONAL_GENDER", "PERSONAL_BIRTHDAY",
        "PERSONAL_MOBILE", "PERSONAL_CITY", "WORK_POSITION", "UF_EMPLOYMENT_DATE",
        "UF_DEPARTMENT", "USER_TYPE"
    ]
    
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    
    all_employees = []
    start = 0
    batch_size = 50
    
    try:
        logger.info("Начало получения сотрудников с пагинацией...")
        
        while True:
            payload = {
                "select": select_fields,
                "start": start
            }
            
            logger.info(f"Запрос сотрудников с start={start}")
            
            response = requests.post(
                bitrix_url,
                json=payload,
                headers=headers,
                timeout=60
            )
            
            if response.status_code == 200:
                data = response.json()
                
                if 'error' in data:
                    error_msg = data.get('error_description', 'Неизвестная ошибка')
                    logger.error(f"Ошибка Bitrix API: {error_msg}")
                    break
                
                employees = data.get('result', [])
                
                if not employees:
                    logger.info("Получены все данные, пагинация завершена")
                    break
                
                all_employees.extend(employees)
                logger.info(f"Получено {len(employees)} сотрудников, всего: {len(all_employees)}")
                
                if len(employees) < batch_size:
                    logger.info("Получена последняя страница данных")
                    break
                
                start += batch_size
                time.sleep(0.2)
                
            else:
                logger.error(f"HTTP ошибка: {response.status_code} - {response.text}")
                break
                
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка подключения: {e}")
        return None
    except Exception as e:
        logger.error(f"Неожиданная ошибка: {e}")
        return None
    
    logger.info(f"Всего получено сотрудников: {len(all_employees)}")
    return all_employees

def save_employees_to_postgres(employees: List[Dict], db_config: Dict):
    """
    Сохраняет сотрудников в PostgreSQL с UPSERT
    """
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        cursor.execute("CREATE SCHEMA IF NOT EXISTS bitrix;")
        
        create_table_query = """
        CREATE TABLE IF NOT EXISTS bitrix.raw_employees (
            id INTEGER PRIMARY KEY,
            xml_id TEXT,
            active BOOLEAN,
            name TEXT,
            last_name TEXT,
            second_name TEXT,
            email TEXT,
            last_login TIMESTAMP,
            date_register TIMESTAMP,
            personal_gender TEXT,
            personal_birthday DATE,
            personal_mobile TEXT,
            personal_city TEXT,
            work_position TEXT,
            uf_employment_date DATE,
            uf_department JSONB,
            user_type TEXT,
            raw_data JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_raw_employees_active ON bitrix.raw_employees(active);
        CREATE INDEX IF NOT EXISTS idx_raw_employees_email ON bitrix.raw_employees(email);
        CREATE INDEX IF NOT EXISTS idx_raw_employees_department ON bitrix.raw_employees USING GIN (uf_department);
        CREATE INDEX IF NOT EXISTS idx_raw_employees_user_type ON bitrix.raw_employees(user_type);
        """
        cursor.execute(create_table_query)
        logger.info("Таблица bitrix.raw_employees проверена/создана")
        
        upsert_query = """
        INSERT INTO bitrix.raw_employees 
            (id, xml_id, active, name, last_name, second_name, email,
             last_login, date_register, personal_gender, personal_birthday,
             personal_mobile, personal_city, work_position, uf_employment_date,
             uf_department, user_type, raw_data)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) 
        DO UPDATE SET
            xml_id = EXCLUDED.xml_id,
            active = EXCLUDED.active,
            name = EXCLUDED.name,
            last_name = EXCLUDED.last_name,
            second_name = EXCLUDED.second_name,
            email = EXCLUDED.email,
            last_login = EXCLUDED.last_login,
            date_register = EXCLUDED.date_register,
            personal_gender = EXCLUDED.personal_gender,
            personal_birthday = EXCLUDED.personal_birthday,
            personal_mobile = EXCLUDED.personal_mobile,
            personal_city = EXCLUDED.personal_city,
            work_position = EXCLUDED.work_position,
            uf_employment_date = EXCLUDED.uf_employment_date,
            uf_department = EXCLUDED.uf_department,
            user_type = EXCLUDED.user_type,
            raw_data = EXCLUDED.raw_data,
            updated_at = CURRENT_TIMESTAMP
        """
        
        success_count = 0
        batch_size = 50
        
        for i in range(0, len(employees), batch_size):
            batch = employees[i:i + batch_size]
            batch_success = 0
            
            for employee in batch:
                try:
                    def parse_date(date_str):
                        if not date_str:
                            return None
                        try:
                            return datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S%z')
                        except:
                            try:
                                return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
                            except:
                                try:
                                    return datetime.strptime(date_str, '%Y-%m-%d')
                                except:
                                    return None
                    
                    uf_department = employee.get('UF_DEPARTMENT')
                    if isinstance(uf_department, list):
                        uf_department_json = json.dumps(uf_department)
                    elif uf_department:
                        uf_department_json = json.dumps([uf_department])
                    else:
                        uf_department_json = None
                    
                    employee_data = (
                        employee.get('ID'),
                        employee.get('XML_ID'),
                        employee.get('ACTIVE') == 'Y',
                        employee.get('NAME'),
                        employee.get('LAST_NAME'),
                        employee.get('SECOND_NAME'),
                        employee.get('EMAIL'),
                        parse_date(employee.get('LAST_LOGIN')),
                        parse_date(employee.get('DATE_REGISTER')),
                        employee.get('PERSONAL_GENDER'),
                        parse_date(employee.get('PERSONAL_BIRTHDAY')),
                        employee.get('PERSONAL_MOBILE'),
                        employee.get('PERSONAL_CITY'),
                        employee.get('WORK_POSITION'),
                        parse_date(employee.get('UF_EMPLOYMENT_DATE')),
                        uf_department_json,
                        employee.get('USER_TYPE'),
                        json.dumps(employee, ensure_ascii=False)
                    )
                    
                    if employee_data[0] is None:
                        logger.warning(f"Пропущен сотрудник с отсутствующим ID: {employee}")
                        continue
                    
                    cursor.execute(upsert_query, employee_data)
                    batch_success += 1
                    success_count += 1
                    
                except Exception as e:
                    logger.error(f"Ошибка при сохранении сотрудника ID {employee.get('ID')}: {e}")
            
            conn.commit()
            logger.info(f"Сохранено пачки {i//batch_size + 1}: {batch_success} сотрудников")
        
        logger.info(f"Успешно сохранено {success_count} из {len(employees)} сотрудников")
            
    except psycopg2.Error as e:
        logger.error(f"Ошибка PostgreSQL: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()

def print_employees_info(employees: List[Dict]):
    """
    Выводит информацию о полученных сотрудниках
    """
    if not employees:
        logger.info("Нет сотрудников для отображения")
        return
    
    print("\n" + "="*70)
    print("ПОЛУЧЕННЫЕ СОТРУДНИКИ")
    print("="*70)
    
    for i, employee in enumerate(employees[:5], 1):
        full_name = f"{employee.get('LAST_NAME', '')} {employee.get('NAME', '')} {employee.get('SECOND_NAME', '')}".strip()
        position = f", Должность: {employee.get('WORK_POSITION')}" if employee.get('WORK_POSITION') else ""
        active_status = " (активен)" if employee.get('ACTIVE') == 'Y' else " (неактивен)"
        print(f"{i}. ID: {employee.get('ID')}, Имя: {full_name}{position}{active_status}")
    
    if len(employees) > 5:
        print(f"... и еще {len(employees) - 5} сотрудников")
    print("="*70)

def main():
    """
    Основная функция
    """
    DB_CONFIG = {
        'host': host,
        'port': port,
        'database': database,
        'user': db_user,
        'password': db_password
    }
    
    logger.info("Запуск скрипта получения сотрудников из Bitrix24 с пагинацией...")
    
    employees = get_bitrix_employees_paginated()
    
    if employees:
        print_employees_info(employees)
        save_employees_to_postgres(employees, DB_CONFIG)
        logger.info("Скрипт успешно завершен!")
    else:
        logger.error("Не удалось получить сотрудников из Bitrix24")

if __name__ == "__main__":
    main()