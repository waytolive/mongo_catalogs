"""
Создание БД для хранения Каталога землетрясений Камчатки и Командорских островов (1962 г. – наст. вр.)
"""

import pymongo
from pymongo import MongoClient, ASCENDING, DESCENDING
from datetime import datetime
import sys
import os




def connect_to_mongodb():
    """Подключение к MongoDB с обработкой ошибок"""
    try:
        uri = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/admin"
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        # Проверка подключения
        client.admin.command('ping')
        print(f"✓ Успешное подключение к MongoDB ({MONGO_HOST}:{MONGO_PORT})")
        return client
    except Exception as e:
        print(f"✗ Ошибка подключения к MongoDB: {e}")
        sys.exit(1)