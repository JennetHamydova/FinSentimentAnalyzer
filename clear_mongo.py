from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
import logging

# --- Konfiguracja ---
# Adres serwera MongoDB (nazwa serwisu w Docker Compose)
# Użyj tych samych danych uwierzytelniających co w skryptach producenta i konsumenta
MONGO_USER = 'root'
MONGO_PASSWORD = 'admin'
MONGO_HOST = 'mongo'
MONGO_PORT = 27017
MONGO_AUTH_SOURCE = 'admin'

# URI połączenia z uwierzytelnieniem
MONGO_URI = f'mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/?authSource={MONGO_AUTH_SOURCE}'

# Nazwa bazy danych i kolekcji do wyczyszczenia
MONGO_DB_NAME = 'financial_sentiment_db'
MONGO_COLLECTIONS_TO_CLEAR = ['processed_links', 'aggregated_sentiment'] # Lista kolekcji do usunięcia danych

# --- Konfiguracja Logowania ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# --- Główna logika czyszczenia ---
logging.info("Uruchamianie skryptu czyszczącego bazy MongoDB...")

try:
    # Inicjalizacja klienta MongoDB z uwierzytelnieniem
    mongo_client = MongoClient(MONGO_URI)
    # Sprawdzenie połączenia i uwierzytelnienia
    mongo_client.admin.command('ping')
    db = mongo_client[MONGO_DB_NAME]

    logging.info(f"Połączono i uwierzytelniono z MongoDB: {MONGO_HOST}:{MONGO_PORT}, baza: {MONGO_DB_NAME}")

    # Czyszczenie określonych kolekcji
    for collection_name in MONGO_COLLECTIONS_TO_CLEAR:
        if collection_name in db.list_collection_names():
            logging.info(f"Czyszczenie kolekcji: {collection_name}...")
            result = db[collection_name].delete_many({})
            logging.info(f"Usunięto {result.deleted_count} dokumentów z kolekcji {collection_name}.")
        else:
            logging.warning(f"Kolekcja {collection_name} nie istnieje w bazie {MONGO_DB_NAME}. Pomijam czyszczenie.")

    logging.info("Czyszczenie bazy MongoDB zakończone.")

except (ConnectionFailure, OperationFailure) as e:
    logging.error(f"Błąd podczas łączenia lub uwierzytelniania z MongoDB: {e}")
    logging.error("Upewnij się, że podane dane uwierzytelniające MongoDB są poprawne.")
except Exception as e:
    logging.error(f"Nieoczekiwany błąd podczas czyszczenia bazy MongoDB: {e}")
finally:
    if 'mongo_client' in locals() and mongo_client:
        mongo_client.close()
        logging.info("Połączenie z MongoDB zamknięte.")

