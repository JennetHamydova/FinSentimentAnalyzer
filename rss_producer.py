import feedparser
import json
import time
import logging
from confluent_kafka import Producer
from transformers import pipeline
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
from datetime import datetime, timezone

# --- Konfiguracja ---
# Adres brokera Kafka (nazwa serwisu w Docker Compose)
KAFKA_BROKER = 'broker:9092'
# Nazwa tematu Kafka
KAFKA_TOPIC = 'financial_headlines_pl'

# Adres serwera MongoDB (nazwa serwisu w Docker Compose)
# UWAGA: Zaktualizuj poniższe dane o swoje rzeczywiste dane uwierzytelniające MongoDB
MONGO_USER = 'root' # <--- Zaktualizowano na podstawie docker-compose.yaml
MONGO_PASSWORD = 'admin'     # <--- Zaktualizowano na podstawie docker-compose.yaml
MONGO_HOST = 'mongo'
MONGO_PORT = 27017
MONGO_AUTH_SOURCE = 'admin' # Zazwyczaj baza danych, w której zdefiniowano użytkownika (często 'admin')

# URI połączenia z uwierzytelnieniem
MONGO_URI = f'mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/?authSource={MONGO_AUTH_SOURCE}'

# Nazwa bazy danych i kolekcji do śledzenia przetworzonych linków
MONGO_DB_NAME = 'financial_sentiment_db'
MONGO_COLLECTION_PROCESSED = 'processed_links'
# Nazwa kolekcji do zapisywania zagregowanych wyników sentymentu (używana przez consumer, ale nazwa jest tu)
MONGO_COLLECTION_AGGREGATED = 'aggregated_sentiment'

# Lista adresów RSS do monitorowania
RSS_FEEDS = {
    'Bloomberg': 'https://feeds.bloomberg.com/markets/news.rss',
    # Dodaj inne polskie feedy RSS, jeśli są dostępne i pasują do tematyki
    # Na potrzeby demo używamy angielskich feedów z PoC
    'Yahoo Finance': 'https://finance.yahoo.com/news/rssindex',
    'The Guardian Business': 'https://www.theguardian.com/uk/business/rss',
    'Google News Finance': 'https://news.google.com/rss/topics/CAAqIQgKIhtDQkFTRGdvSUwyMHZNREpmTjNRU0FtVnVLQUFQAQ?hl=en-US&gl=US&ceid=US%3Aen'
}
# Interwał między cyklami pobierania danych (w sekundach)
FETCH_INTERVAL_SECONDS = 15

# --- Konfiguracja Logowania ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# --- Inicjalizacja Modeli i Klientów ---
try:
    # Inicjalizacja modelu sentymentu (FinBERT)
    # Uwaga: FinBERT jest trenowany na angielskich danych finansowych.
    # Dla polskich nagłówków może być mniej dokładny.
    # W przyszłości warto rozważyć model dla PL lub podejście słownikowe.
    sentiment_pipeline = pipeline("text-classification", model="ProsusAI/finbert")
    logging.info("Model sentymentu załadowany pomyślnie.")
except Exception as e:
    logging.error(f"Błąd podczas ładowania modelu sentymentu: {e}")
    exit()

try:
    # Inicjalizacja producenta Kafka
    producer = Producer({'bootstrap.servers': KAFKA_BROKER})
    logging.info(f"Producent Kafka połączony z {KAFKA_BROKER}")
except Exception as e:
    logging.error(f"Błąd podczas łączenia z Kafka: {e}")
    exit()

try:
    # Inicjalizacja klienta MongoDB z uwierzytelnieniem
    mongo_client = MongoClient(MONGO_URI)
    # Sprawdzenie połączenia i uwierzytelnienia przez wykonanie prostej operacji
    mongo_client.admin.command('ping')
    db = mongo_client[MONGO_DB_NAME]
    processed_collection = db[MONGO_COLLECTION_PROCESSED]
    # Utworzenie indeksu na polu 'link' dla szybszego wyszukiwania
    processed_collection.create_index("link", unique=True)
    logging.info(f"Połączono i uwierzytelniono z MongoDB: {MONGO_HOST}:{MONGO_PORT}, baza: {MONGO_DB_NAME}")
except (ConnectionFailure, OperationFailure) as e:
    logging.error(f"Błąd podczas łączenia lub uwierzytelniania z MongoDB: {e}")
    logging.error("Upewnij się, że podane dane uwierzytelniające MongoDB są poprawne.")
    exit()
except Exception as e:
    logging.error(f"Nieoczekiwany błąd podczas łączenia z MongoDB: {e}")
    exit()


# --- Funkcja pomocnicza do asynchronicznego potwierdzenia dostarczenia wiadomości ---
def delivery_report(err, msg):
    """Wywoływana po dostarczeniu wiadomości do Kafki lub w przypadku błędu."""
    if err is not None:
        logging.error(f'Dostarczenie wiadomości nie powiodło się: {err}')
    else:
        logging.info(f'Wiadomość dostarczona do {msg.topic()} [{msg.partition()}] @ {msg.offset()}')

# --- Główna logika pobierania, analizy i wysyłania ---
def fetch_and_process_rss(feed_name, feed_url):
    """Pobiera feed RSS, analizuje sentyment i wysyła nowe nagłówki do Kafki."""
    logging.info(f"Pobieranie feedu: {feed_name} z {feed_url}")
    try:
        feed = feedparser.parse(feed_url)

        if feed.bozo:
             logging.warning(f"Błąd podczas parsowania feedu {feed_name}: {feed.bozo_exception}")
             # Spróbuj kontynuować, ale z ostrzeżeniem

        new_entries_count = 0
        for entry in feed.entries:
            # Używamy entry.link jako unikalnego identyfikatora
            link = entry.link
            # Używamy entry.title jako tekstu do analizy sentymentu
            title = entry.title

            # Sprawdzenie, czy link był już przetwarzany w MongoDB
            # Używamy kolekcji processed_collection zainicjalizowanej globalnie
            if processed_collection.find_one({'link': link}):
                # logging.debug(f"Link już przetworzony: {link}")
                continue # Pomiń, jeśli już przetworzono

            logging.info(f"Nowy nagłówek znaleziony: {title}")

            try:
                # Analiza sentymentu nagłówka
                # Używamy sentiment_pipeline zainicjalizowanego globalnie
                sentiment_result = sentiment_pipeline(title)[0]
                sentiment_label = sentiment_result['label']
                sentiment_score = sentiment_result['score']

                logging.info(f"Sentyment dla '{title[:50]}...': {sentiment_label} ({sentiment_score:.4f})")

                # Przygotowanie danych do wysłania do Kafki
                # Używamy timestampu z RSS, jeśli dostępny, inaczej aktualny czas UTC
                published_time = datetime.now(timezone.utc).isoformat()
                if hasattr(entry, 'published_parsed'):
                    try:
                        # Parsowanie daty z feeda i konwersja na format ISO 8601 z UTC
                        published_time = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc).isoformat()
                    except Exception as dt_e:
                        logging.warning(f"Nie udało się sparsować daty '{getattr(entry, 'published', 'N/A')}' dla {link}: {dt_e}. Używam aktualnego czasu UTC.")


                message = {
                    'timestamp': published_time,
                    'source': feed_name,
                    'headline': title,
                    'url': link,
                    'sentiment_label': sentiment_label,
                    'sentiment_score': sentiment_score
                }

                # Wysłanie wiadomości do Kafki
                # Używamy producenta zainicjalizowanego globalnie
                producer.produce(
                    KAFKA_TOPIC,
                    key=link.encode('utf-8'), # Użyj linku jako klucza, aby wiadomości z tego samego linku trafiły na tę samą partycję (jeśli Kafka jest partycjonowana)
                    value=json.dumps(message).encode('utf-8'),
                    callback=delivery_report # Callback do potwierdzenia dostarczenia
                )
                new_entries_count += 1

                # Zapisanie linku do MongoDB jako przetworzonego
                # Używamy kolekcji processed_collection zainicjalizowanej globalnie
                processed_collection.insert_one({'link': link, 'processed_at': datetime.now(timezone.utc)})

            except Exception as e:
                logging.error(f"Błąd podczas przetwarzania nagłówka '{title}': {e}")
                # Kontynuuj przetwarzanie kolejnych nagłówków nawet po błędzie

        # Wymuszenie wysłania wszystkich zakolejkowanych wiadomości przed kolejnym cyklem
        producer.flush()
        logging.info(f"Zakończono przetwarzanie feedu {feed_name}. Wysyłano {new_entries_count} nowych nagłówków.")

    except Exception as e:
        logging.error(f"Ogólny błąd podczas pobierania lub przetwarzania feedu {feed_name}: {e}")

# --- Główna pętla programu ---
if __name__ == "__main__":
    logging.info("Uruchamianie RSS Scrapera i Producenta Kafka...")
    while True:
        for name, url in RSS_FEEDS.items():
            fetch_and_process_rss(name, url)
        logging.info(f"Czekam {FETCH_INTERVAL_SECONDS} sekund przed kolejnym cyklem...")
        time.sleep(FETCH_INTERVAL_SECONDS)

