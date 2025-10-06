import sqlite3
import os 
from datetime import datetime
import pandas as pd
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('database.log', encoding='utf-8'),  # 💾 В файл
        logging.StreamHandler()  # 📺 В консоль
    ]
)

class NewsDatabase:
    def __init__(self, db_name='news.db'):
        full_path = os.path.abspath(db_name)
        logging.info(f"Создаем БД: {full_path}")
        
        self.connection = sqlite3.connect(db_name)
        self.cursor = self.connection.cursor()
        self.create_table()

    def create_table(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS news (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT,
            topic TEXT,
            title TEXT NOT NULL,
            text TEXT NOT NULL,
            date DATE
        );
        """
        self.cursor.execute(create_table_query)
        self.connection.commit()
        logging.info("Таблица 'news' создана")

    def close_connection(self):
        self.connection.close()
        logging.info("Соединение с БД закрыто")

    def import_csv_to_db(self, csv_file='parser.csv'):  # Добавил self
        logging.info(f"Импортируем данные из {csv_file}")
        
        if not os.path.exists(csv_file):
            logging.error(f"Файл {csv_file} не найден")
            return

        df = pd.read_csv(csv_file)
        logging.info(f"Прочитано {len(df)} записей из CSV")
        
        df['source'] = 'lenta.ru'
        df['topic'] = 'Новости' 
        df['date'] = pd.Timestamp.now().strftime('%Y-%m-%d')
        
        df.to_sql('news', self.connection, if_exists='append', index=False)
        logging.info(f"Импортировано {len(df)} записей в БД")

if __name__ == "__main__":
    logging.info("Запуск скрипта")
    
    db = NewsDatabase()
    db.import_csv_to_db()
    
    db_file = 'news.db'
    if os.path.exists(db_file):
        logging.info(f"БД создана: {os.path.getsize(db_file)} байт")
    else:
        logging.error("Файл БД не найден")
    
    db.close_connection()
    logging.info("Скрипт завершен")