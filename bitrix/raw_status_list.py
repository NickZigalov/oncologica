import requests
import psycopg2
import json
import logging
from typing import Dict, List, Optional
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
bitrix_url=url+"crm.status.list"

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_bitrix_statuses() -> Optional[List[Dict]]:
    """
    Получает статусы из Bitrix24 API (без пагинации)
    """
    
    # Поля для выборки
    select_fields = [
        "ID", "ENTITY_ID", "STATUS_ID", "NAME", "NAME_INIT", 
        "SORT", "SYSTEM", "SEMANTICS", "CATEGORY_ID"
    ]
    
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    
    try:
        logger.info("Запрос статусов из Bitrix24...")
        
        payload = {
            "select": select_fields
        }
        
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
                return None
            
            statuses = data.get('result', [])
            logger.info(f"Получено статусов: {len(statuses)}")
            return statuses
        else:
            logger.error(f"HTTP ошибка: {response.status_code} - {response.text}")
            return None
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка подключения: {e}")
        return None
    except Exception as e:
        logger.error(f"Неожиданная ошибка: {e}")
        return None

def save_statuses_to_postgres(statuses: List[Dict], db_config: Dict):
    """
    Сохраняет статусы в PostgreSQL с UPSERT
    """
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        cursor.execute("CREATE SCHEMA IF NOT EXISTS bitrix;")
        
        create_table_query = """
        CREATE TABLE IF NOT EXISTS bitrix.raw_status_list (
            id SERIAL PRIMARY KEY,
            status_id VARCHAR(100) UNIQUE NOT NULL,
            entity_id VARCHAR(100),
            name TEXT,
            name_init TEXT,
            sort INTEGER,
            system BOOLEAN,
            semantics VARCHAR(50),
            category_id VARCHAR(100),
            raw_data JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_raw_status_list_status_id ON bitrix.raw_status_list(status_id);
        CREATE INDEX IF NOT EXISTS idx_raw_status_list_entity_id ON bitrix.raw_status_list(entity_id);
        CREATE INDEX IF NOT EXISTS idx_raw_status_list_category_id ON bitrix.raw_status_list(category_id);
        CREATE INDEX IF NOT EXISTS idx_raw_status_list_semantics ON bitrix.raw_status_list(semantics);
        """
        cursor.execute(create_table_query)
        logger.info("Таблица bitrix.raw_status_list проверена/создана")
        
        upsert_query = """
        INSERT INTO bitrix.raw_status_list 
            (status_id, entity_id, name, name_init, sort, system, semantics, category_id, raw_data)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (status_id) 
        DO UPDATE SET
            entity_id = EXCLUDED.entity_id,
            name = EXCLUDED.name,
            name_init = EXCLUDED.name_init,
            sort = EXCLUDED.sort,
            system = EXCLUDED.system,
            semantics = EXCLUDED.semantics,
            category_id = EXCLUDED.category_id,
            raw_data = EXCLUDED.raw_data,
            updated_at = CURRENT_TIMESTAMP
        """
        
        success_count = 0
        
        for status in statuses:
            try:
                # Используем STATUS_ID как первичный ключ, с fallback на ID
                status_id = status.get('STATUS_ID') or status.get('ID')
                
                status_data = (
                    status_id,
                    status.get('ENTITY_ID'),
                    status.get('NAME'),
                    status.get('NAME_INIT'),
                    status.get('SORT'),
                    status.get('SYSTEM'),
                    status.get('SEMANTICS'),
                    status.get('CATEGORY_ID'),
                    json.dumps(status, ensure_ascii=False)
                )
                
                if status_data[0] is None:
                    logger.warning(f"Пропущен статус с отсутствующим STATUS_ID: {status}")
                    continue
                
                cursor.execute(upsert_query, status_data)
                success_count += 1
                
            except Exception as e:
                logger.error(f"Ошибка при сохранении статуса ID {status.get('ID')}: {e}")
        
        conn.commit()
        logger.info(f"Успешно сохранено {success_count} из {len(statuses)} статусов")
            
    except psycopg2.Error as e:
        logger.error(f"Ошибка PostgreSQL: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()

def print_statuses_info(statuses: List[Dict]):
    """
    Выводит информацию о полученных статусах
    """
    if not statuses:
        logger.info("Нет статусов для отображения")
        return
    
    print("\n" + "="*70)
    print("ПОЛУЧЕННЫЕ СТАТУСЫ")
    print("="*70)
    
    for i, status in enumerate(statuses[:5], 1):
        print(f"{i}. ID: {status.get('ID')}, STATUS_ID: {status.get('STATUS_ID')}, "
              f"ENTITY_ID: {status.get('ENTITY_ID')}, NAME: {status.get('NAME')}")
    
    if len(statuses) > 5:
        print(f"... и еще {len(statuses) - 5} статусов")
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
    
    logger.info("Запуск скрипта получения статусов из Bitrix24...")
    
    statuses = get_bitrix_statuses()
    
    if statuses:
        print_statuses_info(statuses)
        save_statuses_to_postgres(statuses, DB_CONFIG)
        logger.info("Скрипт успешно завершен!")
    else:
        logger.error("Не удалось получить статусы из Bitrix24")

if __name__ == "__main__":
    main()