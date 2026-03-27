Примеры запросов:
# Найти землетрясения в радиусе 50 км
query = {
    "location": {
        "$near": {
            "$geometry": {"type": "Point", "coordinates": [160.456, 56.123]},
            "$maxDistance": 50000  # метры
        }
    }
}
results = collection.find(query)

# Найти землетрясения за последнюю неделю с Ml > 4
from datetime import datetime, timedelta
query = {
    "originTime": {"$gte": datetime.utcnow() - timedelta(days=7)},
    "magnitudes.ml": {"$gte": 4.0}
}
results = collection.find(query).sort("originTime", -1)

# Найти по зоне
query = {"zone": "Kamchatka"}
results = collection.find(query)