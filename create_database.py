"""
Создание БД для хранения Каталога землетрясений Камчатки и Командорских островов (1962 г. – наст. вр.)
Порционная вставка — данные загружаются батчами по 10 000 записей (защита от переполнения памяти)
Обработка дубликатов — при режиме добавления дубликаты EvId игнорируются
Валидация схемы — некорректные данные отбрасываются на этапе преобразования
Статистика после загрузки — автоматическая проверка качества данных
"""

import sys
import os
import pymongo
from sys import argv
from pymongo import MongoClient, ASCENDING, DESCENDING
from datetime import datetime, timedelta

from from_seisfun import config_read, query_postgresql

CATALOGUE_SELECT = ('SELECT "evName", "hypDate", "hypDateError", "hypLatitude", "hypLongitude", "hypHypocenterError",'
                    ' "hypDepth", "hypDepthError", m_ks."hmgnValue" as "Ks", m_ks."hmgnValue"*0.5-0.75 as "Ml",'
                    ' m_mc."hmgnValue" as "Mc", NULL as "Mw", "agncShortName", "softNameLat", "zstructShortName",'
                    ' "zlayShortName", "zvolcName", "hypComment", "hypId", "hypEvId", "hypSubmitTime", "hypUpdated"')
# 19.5 seconds
CATALOGUE_FROM_WHERE = ('FROM "Event" JOIN "EventState" ON "EventState"."evstEvId" = "Event"."evId" '
                  'AND "EventState"."evstValue" = 2 AND "EventState"."evstAgncId" = (( SELECT min(a."evstAgncId") AS min '
                  'FROM "EventState" a WHERE a."evstEvId" = "Event"."evId" AND a."evstValue" = 2 '
                  'AND (a."evstAgncId" = ANY (ARRAY[2, 3])))) '
                  'JOIN "Hypocenter" ON "Event"."evId" = "Hypocenter"."hypEvId" AND "Hypocenter"."hypType" = 2 '
                  'AND "EventState"."evstAgncId" = "Hypocenter"."hypAgncId" '
                  'AND ("Hypocenter"."hypAgncId" = ANY (ARRAY[2, 3])) '
                  'LEFT JOIN "Software" ON "Hypocenter"."hypSoftId" = "Software"."softId" '
                  'LEFT JOIN "HypMagnitude" m_ks ON m_ks."hmgnHypId" = "Hypocenter"."hypId" AND m_ks."hmgnMtId" = 2 '
                  'LEFT JOIN "HypMagnitude" m_mc ON m_mc."hmgnHypId" = "Hypocenter"."hypId" AND m_mc."hmgnMtId" = 3 '
                  'LEFT JOIN "Zone" ON "Zone"."zonHypId" = "Hypocenter"."hypId" AND "Zone"."zonZmosId" = "GetZoneLatestMosaic"() '
                  'LEFT JOIN "ZoneStructure" ON "Zone"."zonZstructId" = "ZoneStructure"."zstructId" '
                  'LEFT JOIN "ZoneVolcano" ON "Zone"."zonZvolcId" = "ZoneVolcano"."zvolcId" '
                  'LEFT JOIN "ZoneLayer" ON "Zone"."zonZlayId" = "ZoneLayer"."zlayId" '
                  'JOIN "Agency" ON "Hypocenter"."hypAgncId" = "Agency"."agncId" AND "EventState"."evstAgncId" = "Agency"."agncId" '
                  'WHERE "Event"."evState" = 2 ')
# 7.0 seconds
CATALOGUE_COUNT_FROM_WHERE = (' FROM "Event" JOIN "EventState" ON "EventState"."evstEvId" = "Event"."evId" '
                   ' AND "Event"."evState" = 2 AND "EventState"."evstValue" = 2 '
                   ' AND "EventState"."evstAgncId" = (( SELECT min(a."evstAgncId") AS min FROM "EventState" a '
                   ' WHERE a."evstEvId" = "Event"."evId" AND a."evstValue" = 2 AND (a."evstAgncId" = ANY (ARRAY[2, 3])))) '
                   ' JOIN "Hypocenter" ON "Event"."evId" = "Hypocenter"."hypEvId" AND "Hypocenter"."hypType" = 2 '
                   ' AND "EventState"."evstAgncId" = "Hypocenter"."hypAgncId" ')
# 6.5 seconds
CATALOGUE_COUNT_FROM_WHERE_qwen = (' FROM ( SELECT DISTINCT ON ("es"."evstEvId") "es"."evstEvId", "es"."evstAgncId" '
                        'FROM "EventState" "es" WHERE "es"."evstValue" = 2 AND "es"."evstAgncId" = ANY (ARRAY[2, 3]) '
                        'ORDER BY "es"."evstEvId", "es"."evstAgncId" ASC ) AS "es_min" '
                        'JOIN "Event" ON "Event"."evId" = "es_min"."evstEvId" '
                        'JOIN "Hypocenter" ON "Event"."evId" = "Hypocenter"."hypEvId" '
                        'AND "es_min"."evstAgncId" = "Hypocenter"."hypAgncId" '
                        'WHERE "Event"."evState" = 2 AND "Hypocenter"."hypType" = 2 ')


def connect_to_mongodb(**kwargs):
    """Подключение к MongoDB с обработкой ошибок"""
    host = kwargs.get('host','')
    port = kwargs.get('port', '')
    user = kwargs.get('user', '')
    password = kwargs.get('password', '')
    try:
        uri = f"mongodb://{user}:{password}@{host}:{port}/admin"
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        # Проверка подключения
        client.admin.command('ping')
        return client
    except Exception as e:
        print(f"✗ Ошибка подключения к MongoDB: {e}")
        sys.exit(1)


def drop_existing_collection(db, collection_name: str):
    """Удаление существующей коллекции"""
    try:
        if collection_name in db.list_collection_names():
            count = db[collection_name].count_documents({})
            print(f"⚠ Найдена существующая коллекция '{collection_name}' с {count} документами")

            # Подтверждение удаления (можно отключить для автоматического режима)
            confirm = input("➤ Удалить существующую коллекцию? (yes/no): ").strip().lower()
            if confirm != 'yes':
                print("✗ Операция отменена пользователем")
                sys.exit(0)

            db.drop_collection(collection_name)
            print(f"✓ Коллекция '{collection_name}' успешно удалена")
        else:
            print(f"ℹ Коллекция '{collection_name}' не существует, будет создана новая")
        return True

    except Exception as e:
        print(f"✗ Ошибка удаления коллекции: {e}")
        sys.exit(1)

def create_collection_with_validation(db, collection_name):
    timer = datetime.now()
    """Создание коллекции с валидацией схемы"""
    try:
        validator = {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["evId", "hypId", "evName", "hypDate", "location"],
                "properties": {
                    "evId": {"bsonType": "number"},
                    "hypId": {"bsonType": "number"},
                    "evName": {"bsonType": "string", "maxLength": 16},
                    "hypDate": {"bsonType": "date"},
                    "location": {
                        "bsonType": "object",
                        "required": ["type", "coordinates"],
                        "properties": {
                            "type": {"enum": ["Point"]},
                            "coordinates": {"bsonType": "array","minItems": 2,"maxItems": 2,
                                            "items": {"bsonType": "number"}
                            }
                        }
                    },
                    "hypDepth": {"bsonType": ["number", "null"]},
                    "hypDepthError": {"bsonType": ["number", "null"], "minimum": 0},
                    "hypHypocenterError": {"bsonType": ["number", "null"], "minimum": 0},
                    "magnitudes": {
                        "bsonType": "object",
                        "properties": {
                            "Ml": {"bsonType": ["number", "null"]},
                            "Mc": {"bsonType": ["number", "null"]},
                            "Ks": {"bsonType": ["number", "null"]},
                            "Mw": {"bsonType": ["number", "null"]}
                        }
                    },
                    "zstructShortName": {"bsonType": ["string", "null"], "maxLength": 2},
                    "zlayShortName": {"bsonType": ["string", "null"], "maxLength": 2},
                    "zone": {"bsonType": ["string", "null"]},
                    "zvolcName": {"bsonType": ["string", "null"]},
                    "agncShortName": {"bsonType": "string"},
                    "softNameLat": {"bsonType": "string"},
                    "hypComment": {"bsonType": ["string", "null"]},
                    "hypSubmitTime": {"bsonType": ["date", "null"]},
                    "hypUpdated": {"bsonType": ["date", "null"]}
                }
            }
        }
        db.create_collection(collection_name, validator=validator)
        print(f"✓ Коллекция '{collection_name}' создана с валидацией схемы [{datetime.now() - timer}]")
        return db[collection_name]

    except Exception as e:
        print(f"✗ Ошибка создания коллекции: {e}")
        sys.exit(1)

def create_indexes(collection):
    timer = datetime.now()
    """Создание необходимых индексов"""
    try:
        indexes = [
            ([("location", "2dsphere")], "idx_location_2dsphere"),
            ([("evId", ASCENDING)], "idx_evId", True),
            ([("evName", ASCENDING)], "idx_evName", True),
            ([("hypId", ASCENDING)], "idx_hypId", True),
            ([("hypDate", DESCENDING), ("magnitudes.Ml", DESCENDING)], "idx_datetime_Ml"),
            ([("hypDate", DESCENDING), ("magnitudes.Mw", DESCENDING)], "idx_datetime_Mw"),
            ([("zone", ASCENDING)], "idx_zone"),
            ([("hypDepth", ASCENDING)], "idx_hypDepth"),
            ([("hypSubmitTime", DESCENDING)], "idx_hypSubmitTime"),
            ([("hypUpdated", DESCENDING)], "idx_hypUpdated"),
        ]
        print(f"Создан индекс: ", end='')
        for idx_def in indexes:
            if len(idx_def) == 2:
                keys, name = idx_def
                collection.create_index(keys, name=name)
            else:
                keys, name, unique = idx_def
                collection.create_index(keys, name=name, unique=unique)
            print(f"{name}; ", end='')
    except Exception as e:
        print("\n" + f"✗ Ошибка создания индексов: {e}")
    print("\n" + f"✓ Создание индексов завершено [{datetime.now() - timer}]")

def transform_row(row):
    """Преобразование строки CSV в документ MongoDB"""
    try:
        doc = {
            "evId": row['hypEvId'],
            "hypId": row['hypId'],
            "evName": row['evName'],
            "hypDate": row['hypDate'],
            "location": {
                "type": "Point",
                "coordinates": [float(row['hypLongitude']), float(row['hypLatitude'])]
                },
            "hypDepth": float(row['hypDepth']),
            "hypDepthError": float(row['hypDepthError']) if row['hypDepthError'] is not None else None,
            "hypHypocenterError": float(row['hypHypocenterError']) if row['hypHypocenterError'] is not None else None,
            "magnitudes": {
                "Ml": float(row['Ml']) if row['Ml'] is not None else None,
                "Mc": float(row['Mc']) if row['Mc'] is not None else None,
                "Ks": float(row['Ks']) if row['Ks'] is not None else None,
                "Mw": float(row['Mw']) if row['Mw'] is not None else None,
                },
            "zstructShortName": row['zstructShortName'],
            "zlayShortName": row['zlayShortName'],
            "zone": row['zstructShortName'] + ('.' + row['zlayShortName']) if row['zlayShortName'] else '',
            "zvolcName": row['zvolcName'],
            "agncShortName": row['agncShortName'],
            "softNameLat": row['softNameLat'],
            "hypComment": row['hypComment'],
            "hypSubmitTime": row['hypSubmitTime'],
            "hypUpdated": row['hypUpdated']
        }
        return doc
    except Exception as e:
        print(f"evName:{row['evName']} [hypId:{row['hypId']}]. {e}")
        return None


def load_data(collection, batch_size: int = 10000):
    timer = datetime.now()
    try:
        """Загрузка данных из ЕИССД"""
        query = (f' {CATALOGUE_SELECT} {CATALOGUE_FROM_WHERE} '
                 # ' AND ( "hypDate" >= \'1962.01.01 00:00:00\' AND "hypDate" <= \'2027.01.01 23:59:59\' )'
                 )
        results = query_postgresql(query=query , options={'typeOut': 'dictionary'})
        print(f"✓ Загружено {len(results)} записей из ЕИССД [{datetime.now() - timer}]")
        documents = []
        failed = 0
        for row in results:
            doc = transform_row(row)
            if doc:
                documents.append(doc)
            else:
                failed += 1
        print(f"✓ Преобразовано {len(documents)}, ошибок: {failed}")
        if not documents:
            print("✗ Нет данных для вставки")
            return
        # Вставка порциями по batch_size документов
        inserted = 0
        for j in range(0, len(documents), batch_size):
            batch = documents[j:j + batch_size]
            # result = collection.insert_many(batch, ordered=False)
            try:
                result = collection.insert_many(batch, ordered=False)
                inserted += len(result.inserted_ids)
                print(f"  → Вставлено {inserted}/{len(documents)} документов")
            # except pymongo.errors.BulkWriteError as e:
            #     print(f"  ⚠ Частичная вставка: {inserted} документов:")
            #     print(e.details)  # Покажет, какие именно документы не прошли
            # except Exception as e:
            #     print(f"Другая ошибка: {e}")
            except pymongo.errors.BulkWriteError as bwe:
                inserted += bwe.details.get('nInserted', 0)
                print(f"  ⚠ Частичная вставка: {inserted} документов")
                print(bwe.details)  # Покажет, какие именно документы не прошли

        if len(results) == inserted:
            note = f"✓ Вставлены все документы"
        else:
            note = f"✓ Всего вставлено: {inserted} из {len(results)} документов"
        print(f"{note} [{datetime.now() - timer}]")

    except Exception as e:
        print(f"✗ Ошибка загрузки данных: {e}")
        sys.exit(1)


def sync_data(collection, batch_size: int = 10000):
    """
    Инкрементальная синхронизация данных из PostgreSQL в MongoDB.
    Логика:
    1. Получает максимальные значения hypSubmitTime и hypUpdated из MongoDB
    2. Запрашивает из PostgreSQL записи, у которых эти поля больше максимумов
    3. Для записей с существующим evId — обновляет документ
    4. Для новых evId — вставляет новый документ
    :param collection: объект коллекции MongoDB
    :param batch_size: размер пакета для bulk-операций
    """
    timer = datetime.now()
    # =====================================================
    # ШАГ 1: Получаем максимальные значения времени из MongoDB
    # =====================================================
    # Получение максимальных значений времени из MongoDB
    max_submit = collection.find_one({}, {"hypSubmitTime": 1}, sort=[("hypSubmitTime", DESCENDING)])
    max_updated = collection.find_one({}, {"hypUpdated": 1}, sort=[("hypUpdated", DESCENDING)])
    max_submit_time = max_submit.get("hypSubmitTime") if max_submit else None
    max_updated_time = max_updated.get("hypUpdated") if max_updated else None
    # в mongoDB округление времени до миллисекунд, из-за чего поле hypUpdated последнего события в postgrey (микросекунды)
    # будет больше чем в mongoDB. Два варианта:
    # 1. Округление max_updated_time до миллисекунды, но есть вероятность пропустить событие в тех же миллисекундах
    # max_updated_time = max_updated_time + timedelta(milliseconds=1)  # прибавим 1 миллисекунду к max_updated_time
    # 2. Забить на то что всегда будет обновляться последний элемент (выбран это вариант)
    # Если коллекция пуста — берем все данные
    if max_submit_time is None and max_updated_time is None:
        # Подтверждение удаления (можно отключить для автоматического режима)
        confirm = input("  ℹ Коллекция пуста — загружать все данные? (yes/no): ").strip().lower()
        if confirm != 'yes':
            print("✗ Операция отменена пользователем")
            sys.exit(0)
        time_filter = "1=1"  # Нет фильтра, берем всё
    else:
        # Формируем условие фильтрации по времени
        conditions = []
        if max_submit_time:
            conditions.append(f'"hypSubmitTime" > \'{max_submit_time}\'')
        if max_updated_time:
            conditions.append(f'"hypUpdated" >= \'{max_updated_time}\'')  # >= потому что прибавили 1 миллисекунду
        time_filter = " OR ".join(conditions) if conditions else "1=1"
        print(f"Инкрементальная синхронизация. Фильтр времени: {time_filter}")

    # =====================================================
    # ШАГ 2: Запрос данных из PostgreSQL
    # =====================================================
    # print("Загрузка данных из PostgreSQL...")
    query = (f'{CATALOGUE_SELECT} {CATALOGUE_FROM_WHERE} AND ({time_filter}) '
             f'ORDER BY "hypSubmitTime" ASC NULLS FIRST, "hypUpdated" ASC NULLS FIRST')  #FIRST/LAST

    # timer_query = datetime.now()
    results = query_postgresql(query=query, options={'typeOut': 'dictionary'})
    # print(f"✓ Загружено {len(results)} записей из PostgreSQL [{datetime.now() - timer_query}]")
    if not results:
        print("ℹ Нет новых данных для синхронизации")
        return
    # =====================================================
    # ШАГ 3: Преобразование и подготовка к upsert
    # =====================================================
    # print("🔄 Преобразование данных...")
    documents = []
    failed = 0
    for row in results:
        doc = transform_row(row)
        if doc:
            documents.append(doc)
        else:
            failed += 1
    len_documents = len(documents)
    # print(f"✓ Подготовлено {len_documents}, ошибок {failed}")
    if not documents:
        print("✗ Нет данных для вставки/обновления")
        return
    elif failed > 0:
        print("✗ Проверьте загруженные данные и повторите операцию")
        return
    # =====================================================
    # ШАГ 4: Bulk upsert операции
    # =====================================================
    # print(f" Выполнение upsert операций (пакетами по {batch_size})...")
    inserted_count = 0
    updated_count = 0
    error_count = 0
    for j in range(0, len_documents, batch_size):
        batch = documents[j:j + batch_size]
        operations = []
        for doc in batch:
            ev_id = doc.get("evId")
            if ev_id is None:
                continue
            # Операция upsert: найти по evId, если есть — заменить, если нет — вставить
            operations.append(
                pymongo.UpdateOne(
                    {"evId": ev_id},  # фильтр (поиск по уникальному полю)
                    {"$set": doc},  # новые данные
                    upsert=True  # создать, если не найден
                )
            )
        if not operations:
            continue
        try:
            result = collection.bulk_write(operations, ordered=False)
            inserted_count += result.upserted_count or 0
            updated_count += result.modified_count or 0
            if result.bulk_api_result and result.bulk_api_result.get('writeErrors'):
                error_count += len(result.bulk_api_result['writeErrors'])
            # processed = min(j + batch_size, len_documents)
            # print(f"✓ Обработано {processed} записей (вставлено: {inserted_count}, обновлено: {updated_count})")

        except pymongo.errors.BulkWriteError as bwe:
            # Детальная информация об ошибках
            details = bwe.details
            inserted_count += details.get('nUpserted', 0)
            updated_count += details.get('nModified', 0)
            error_count += len(details.get('writeErrors', []))
            print(f"  ⚠ Ошибки в пакете: {len(details.get('writeErrors', []))}")
            for err in details.get('writeErrors', [])[:3]:  # Показать первые 3 ошибки
                print(f"    - {err.get('errmsg', 'Unknown error')}")

    # =====================================================
    # ШАГ 5: Итоговая статистика
    # =====================================================
    mongo_total = collection.count_documents({})
    exist_count = len_documents - inserted_count - updated_count - error_count
    print(f"Загружено: {len(results)}  Вставлено: {inserted_count};  Обновлено: {updated_count}; Уже были: {exist_count}; "
          f"Ошибок: {error_count} [{datetime.now() - timer}]")
    print(f"MongoDB: всего записей = {mongo_total}")

    return {
        "processed": len(documents),
        "inserted": inserted_count,
        "updated": updated_count,
        "errors": error_count,
        "total_in_collection": mongo_total
        }

def verify_data(collection, **kwargs):
    timer = datetime.now()
    """Проверка загруженных данных"""
    print("=" * 50)
    total = collection.count_documents({})
    print(f"Проверка данных. Всего записей: {total}")

    if total > 0:
        sample = collection.find_one()
        if sample:
            print(f"  Пример записи:")
            print(f"  EvId: {sample.get('evId')}")
            print(f"  Время: {sample.get('hypDate')}")
            print(f"  Координаты: {sample.get('location', {}).get('coordinates')}")
            print(f"  Глубина: {sample.get('hypDepth')} км")
            print(f"  Магнитуда ML: {sample.get('magnitudes', {}).get('Ml')}")
            print(f"  Зона: {sample.get('zone')}")

        # Статистика по магнитудам
        print(f"\nСтатистика магнитуд:")
        pipeline = [
            {"$match": {"magnitudes.Ml": {"$ne": None}}},
            {"$group": {
                "_id": None,
                "min": {"$min": "$magnitudes.Ml"},
                "max": {"$max": "$magnitudes.Ml"},
                "avg": {"$avg": "$magnitudes.Ml"},
                "count": {"$sum": 1}
            }}
        ]
        result = list(collection.aggregate(pipeline))
        if result:
            stats = result[0]
            print(f"  Мин: {stats['min']:.2f}, Макс: {stats['max']:.2f}, Среднее: {stats['avg']:.2f}."
                  f" Записей с магнитудой: {stats['count']}")

        # Статистика по зонам
        print(f"\nТоп-5 зон по количеству землетрясений:")
        pipeline = [
            {"$group": {"_id": "$zone", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 5}
        ]
        for zone in collection.aggregate(pipeline):
            print(f"  {zone['_id']}: {zone['count']}", end="; ")

    print(f"✓ Проверка данных завершена [{datetime.now() - timer}]")


def verify_sync_status(collection, **kwargs):
    timer = datetime.now()
    """Проверка актуальности данных в MongoDB относительно источника"""
    # Максимальные значения в MongoDB
    max_submit = collection.find_one({}, {"hypSubmitTime": 1}, sort=[("hypSubmitTime", DESCENDING)])
    max_updated = collection.find_one({}, {"hypUpdated": 1}, sort=[("hypUpdated", DESCENDING)])
    mongo_max_submit = max_submit.get("hypSubmitTime") if max_submit else None
    mongo_max_updated = max_updated.get("hypUpdated") if max_updated else None
    mongo_total = collection.count_documents({})
    # Запрос к PostgreSQL для сравнения
    query = ('SELECT MAX("hypSubmitTime") as max_submit, MAX("hypUpdated") as max_updated, count(*) as count  '
             f'{CATALOGUE_COUNT_FROM_WHERE_qwen}')
    pg_time = query_postgresql(query, options={'typeOut': 'dictionary'})
    pg_max_submit = pg_time[0]["max_submit"]
    pg_max_updated = pg_time[0]["max_updated"]
    pg_total = pg_time[0]["count"]
    # Проверка рассинхронизации
    if pg_max_submit > mongo_max_submit:
        print(f"В PostgreSQL и MongoDB даты сохранения самых последних событий отличаются!")
        print(f"\"hypSubmitTime\": PostgreSQL = {pg_max_submit}; MongoDB = {mongo_max_submit}")
        print(f"  ⚠ MongoDB отстаёт на {pg_max_submit - mongo_max_submit}")
        return None
    if abs(pg_max_updated - mongo_max_updated) > timedelta(milliseconds=1):
        print(f"В PostgreSQL и MongoDB даты последних изменений событий отличаются!")
        print(f"\"hypUpdated\": PostgreSQL = {pg_max_updated}; MongoDB = {mongo_max_updated}")
        print(f"  ⚠ MongoDB отстаёт на {pg_max_updated - mongo_max_updated}")
        return None
    # число строк:
    if pg_total > mongo_total:
        print(f"Всего записей: PostgreSQL = {pg_total}; MongoDB = {mongo_total}")
        print(f"⚠ Разница в количестве {pg_total - mongo_total} записей")
    print(f"✓ Проверка актуальности данных завершена [{datetime.now() - timer}]")

# ================= MAIN =================
if __name__ == "__main__":
    timer_t = datetime.now()
    # --- входящие переменные ---
    # 'C:\Alhor\Python\mongo_catalogs\.venv\Scripts\python.exe C:\Alhor\Python\mongo_catalogs\create_database.py full true'
    # Аргументы:
    #   1: MODE ('sync' или 'full')
    #   2: REPLACE_EXISTING (False/True)
    if len(argv) > 1:
        MODE = 'full' if argv[1].lower() == 'full' else 'sync'
    else:
        MODE = 'sync'  # по умолчанию полная загрузка
    if len(argv) > 2:
        REPLACE_EXISTING = argv[2].lower() == 'true'
    else:
        REPLACE_EXISTING = False

    # section = 'mongodbwrite'
    section = 'mongodbwrite_test'
    config = config_read('config.ini')
    if config is None or not config.has_section(section):
        print('Error: не найден файл конфигурации или секция!')
        exit()

    mongodb_connect = dict(config[section])
    print("=" * 50)
    print("Загрузка каталога землетрясений в MongoDB")
    print(f"База данных: {mongodb_connect['dbname']}; Коллекция: {mongodb_connect['collection']}")
    print(f"Режим: {MODE.upper()}; Замена коллекции: {'ДА' if REPLACE_EXISTING else 'НЕТ'}")
    print("=" * 50)
    # 1. Подключение
    client = connect_to_mongodb(**mongodb_connect)
    db = client[mongodb_connect['dbname']]
    # Проверка существования коллекции
    try:
        list_collection_names = db.list_collection_names()
    except Exception:
        list_collection_names = None

    # 2. Создание/замена/обновление коллекции
    if MODE == 'full':
        if REPLACE_EXISTING:
            drop_existing_collection(db, mongodb_connect['collection'])
        collection = create_collection_with_validation(db, mongodb_connect['collection'])
    else:
        # Для режима sync — просто получаем коллекцию (создаём если нет)
        if mongodb_connect['collection'] not in db.list_collection_names():
            collection = create_collection_with_validation(db, mongodb_connect['collection'])
        else:
            collection = db[mongodb_connect['collection']]

    # 3. Создание индексов (безопасно — не создаёт дубликаты)
    create_indexes(collection)
    # 4. Загрузка данных в зависимости от режима
    if MODE == 'sync':
        sync_data(collection)  # Инкрементальная синхронизация
    elif MODE == 'full':
        load_data(collection)  # Полная загрузка
    else:
        print(f"✗ Неизвестный режим: {MODE}")
        sys.exit(1)
    # 5. Проверка
    # verify_data(collection)
    verify_sync_status(collection, mode=MODE)
    # Закрытие соединения
    client.close()
    print("\n✓ Соединение закрыто")
    print(f"Общее время: {datetime.now() - timer_t}")