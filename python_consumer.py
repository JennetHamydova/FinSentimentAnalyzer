import json
import time
import logging
from kafka import KafkaConsumer
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
from datetime import datetime, timezone, timedelta
from collections import defaultdict

# --- Konfiguracja ---
# Adres brokera Kafka (nazwa serwisu w Docker Compose)
KAFKA_BROKER = 'broker:9092'
# Nazwa tematu Kafka
KAFKA_TOPIC = 'financial_headlines_pl'
# Grupa konsumentów Kafka
KAFKA_CONSUMER_GROUP = 'sentiment_aggregator_group'

# Adres serwera MongoDB (nazwa serwisu w Docker Compose)
# UWAGA: Zaktualizowane dane uwierzytelniające na podstawie docker-compose.yaml
MONGO_USER = 'root' # <--- Zaktualizowano
MONGO_PASSWORD = 'admin'     # <--- Zaktualizowano
MONGO_HOST = 'mongo'
MONGO_PORT = 27017
MONGO_AUTH_SOURCE = 'admin' # Zazwyczaj baza danych, w której zdefiniowano użytkownika (często 'admin')

# URI połączenia z uwierzytelnieniem
MONGO_URI = f'mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/?authSource={MONGO_AUTH_SOURCE}'

# Nazwa bazy danych i kolekcji do zapisywania zagregowanych wyników sentymentu
MONGO_DB_NAME = 'financial_sentiment_db'
MONGO_COLLECTION_AGGREGATED = 'aggregated_sentiment'

# Konfiguracja okna czasowego dla agregacji w Pythonie
# Agregujemy dane z ostatnich X sekund, co Y sekund będziemy zapisywać wyniki
AGGREGATION_WINDOW_SECONDS = 300 # Okno 5 minut (300 sekund)
AGGREGATION_INTERVAL_SECONDS = 60 # Agreguj i zapisuj co 1 minutę (60 sekund)

# --- Konfiguracja Logowania ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# --- Inicjalizacja Klientów ---
try:
    # Inicjalizacja konsumenta Kafka
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        # Zmieniono 'latest' na 'earliest'
        auto_offset_reset='earliest', # Start od najstarszych dostępnych wiadomości
        enable_auto_commit=True,    # Automatyczne commitowanie offsetów
        group_id=KAFKA_CONSUMER_GROUP,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')) # Deserializacja JSON
    )
    logging.info(f"Konsument Kafka połączony z {KAFKA_BROKER}, czyta z tematu {KAFKA_TOPIC}, grupa {KAFKA_CONSUMER_GROUP}")
except Exception as e:
    logging.error(f"Błąd podczas łączenia z Kafka: {e}")
    exit()

try:
    # Inicjalizacja klienta MongoDB z uwierzytelnieniem
    mongo_client = MongoClient(MONGO_URI)
    # Sprawdzenie połączenia i uwierzytelnienia przez wykonanie prostej operacji
    mongo_client.admin.command('ping')
    db = mongo_client[MONGO_DB_NAME]
    aggregated_collection = db[MONGO_COLLECTION_AGGREGATED]
    # Opcjonalnie: Utworzenie indeksu na polach okna czasowego i etykiety sentymentu dla szybszego wyszukiwania
    aggregated_collection.create_index([("window_start", 1), ("sentiment_label", 1)])
    logging.info(f"Połączono i uwierzytelniono z MongoDB: {MONGO_HOST}:{MONGO_PORT}, baza: {MONGO_DB_NAME}, kolekcja: {MONGO_COLLECTION_AGGREGATED}")
except (ConnectionFailure, OperationFailure) as e:
    logging.error(f"Błąd podczas łączenia lub uwierzytelniania z MongoDB: {e}")
    logging.error("Upewnij się, że podane dane uwierzytelniające MongoDB są poprawne.")
    exit()
except Exception as e:
    logging.error(f"Nieoczekiwany błąd podczas łączenia z MongoDB: {e}")
    exit()

# --- Struktury danych do agregacji w pamięci ---
# Przechowujemy wiadomości w pamięci przez czas trwania okna agregacji
# Klucz: (etykieta_sentymentu, źródło - opcjonalnie, można dodać grupowanie po źródle)
# Wartość: lista wyników sentymentu
message_buffer = []
last_aggregation_time = datetime.now(timezone.utc)

# --- Funkcja do agregacji i zapisu do MongoDB ---
def aggregate_and_save():
    """Agreguje dane z bufora wiadomości w oknach czasowych i zapisuje wyniki do MongoDB."""
    global message_buffer, last_aggregation_time

    logging.info("Rozpoczynanie agregacji i zapisu...")
    current_time = datetime.now(timezone.utc)
    # Określenie początku okna agregacji (np. 5 minut temu)
    window_start_threshold = current_time - timedelta(seconds=AGGREGATION_WINDOW_SECONDS)

    # Filtrowanie wiadomości, które wpadają w aktualne okno agregacji
    # Zachowujemy też wiadomości starsze niż okno, które mogą być potrzebne w następnych oknach
    messages_in_window = [
        msg for msg in message_buffer
        if 'timestamp' in msg and datetime.fromisoformat(msg['timestamp'].replace('Z', '+00:00')) >= window_start_threshold
    ]

    # Grupowanie i agregacja danych w oknie
    # Używamy defaultdict do łatwego grupowania
    aggregated_data = defaultdict(lambda: {'total_score': 0, 'count': 0})

    for msg in messages_in_window:
        label = msg.get('sentiment_label')
        score = msg.get('sentiment_score')
        # Upewnij się, że label i score są dostępne i poprawnego typu
        if label is not None and score is not None and isinstance(score, (int, float)):
             # Można dodać grupowanie po źródle: key = (label, msg.get('source'))
            key = label
            aggregated_data[key]['total_score'] += score
            aggregated_data[key]['count'] += 1
        else:
            logging.warning(f"Wiadomość o nieoczekiwanym formacie lub brakujących polach: {msg}")


    # Przygotowanie danych do zapisu do MongoDB
    records_to_save = []
    # Określamy "koniec okna" jako aktualny czas dla uproszczenia,
    # a "początek okna" jako próg czasowy
    window_end_time = current_time
    window_start_time = window_start_threshold # To jest początek okna, które analizujemy

    for key, data in aggregated_data.items():
        sentiment_label = key # lub (label, source) jeśli grupujemy po źródle
        average_score = data['total_score'] / data['count'] if data['count'] > 0 else 0
        article_count = data['count']

        # Zapisujemy agregację dla każdego typu sentymentu (positive, negative, neutral) w danym oknie
        records_to_save.append({
            'window_start': window_start_time,
            'window_end': window_end_time,
            'sentiment_label': sentiment_label,
            'average_score': average_score,
            'article_count': article_count,
            'aggregated_at': current_time # Kiedy wykonano tę agregację
        })

    # Zapis do MongoDB
    if records_to_save:
        try:
            # Użyj insert_many dla wydajności
            aggregated_collection.insert_many(records_to_save)
            logging.info(f"Zapisano {len(records_to_save)} zagregowanych rekordów do MongoDB.")
        except Exception as e:
            logging.error(f"Błąd podczas zapisu do MongoDB: {e}")
    else:
        logging.info("Brak danych w oknie do agregacji i zapisu.")

    # Aktualizacja bufora wiadomości - usuwamy wiadomości starsze niż próg okna
    message_buffer = messages_in_window
    # Aktualizacja czasu ostatniej agregacji
    last_aggregation_time = current_time
    logging.info(f"Agregacja i zapis zakończone. Wiadomości w buforze: {len(message_buffer)}")


# --- Główna pętla konsumenta ---
logging.info("Uruchamianie konsumenta Kafka i agregatora...")
try:
    while True:
        # Pobieranie wiadomości z Kafki
        # poll() zwraca słownik {TopicPartition: [ConsumerRecord, ...]}
        messages = consumer.poll(timeout_ms=1000, max_records=500) # Czekaj do 1 sekundy, pobierz max 500 rekordów

        if messages:
            for tp, consumer_records in messages.items():
                for consumer_record in consumer_records:
                    message = consumer_record.value # Wiadomość jest już zdeserializowana (słownik Python)
                    #logging.debug(f"Odebrano wiadomość: {message}") # Zbyt dużo logów, gdy działa
                    # Dodaj wiadomość do bufora
                    message_buffer.append(message)
            logging.info(f"Odebrano {sum(len(recs) for recs in messages.values())} nowych wiadomości z Kafki.")

        # Sprawdzenie, czy nadszedł czas na agregację i zapis
        current_time = datetime.now(timezone.utc)
        if (current_time - last_aggregation_time).total_seconds() >= AGGREGATION_INTERVAL_SECONDS:
            aggregate_and_save()

        # Krótka pauza, aby nie obciążać CPU, gdy nie ma wiadomości
        # time.sleep(1) # Usunięto, poll() już czeka

except KeyboardInterrupt:
    logging.info("Konsument zatrzymany przez użytkownika.")
finally:
    consumer.close()
    mongo_client.close()
    logging.info("Połączenia z Kafka i MongoDB zamknięte.")

