import os
import pandas as pd
from azure.eventhub import EventHubProducerClient, EventData

# CSV verisinin bulunduğu dosya
DATA_FILE = "/app/data/hotel_raw_stream.csv"

# Event Hub konfigürasyonu
EVENTHUB_NAMESPACE = os.environ.get("EVENTHUB_NAMESPACE")
EVENTHUB_SAS_KEY = os.environ.get("EVENTHUB_SAS_KEY")
EVENTHUB_KEY_NAME = os.environ.get("EVENTHUB_KEY_NAME")
EVENTHUB_NAME = os.environ.get("EVENTHUB_NAME")


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """CSV’den gelen veriyi temizler"""
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            df[col] = pd.to_numeric(df[col], errors='coerce')
        elif "date" in col.lower() or "timestamp" in col.lower():
            df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
        else:
            df[col] = df[col].astype(str).str.strip()
            # Boş, ???, -- gibi hatalı değerleri None yap
            df[col] = df[col].replace({"": None, "???": None, "--": None, "Five": 5})
            df[col] = df[col].str.replace(r'\s+', ' ', regex=True)
            df[col] = df[col].str.replace(r',+', ',', regex=True)
    return df


def send_batch(df: pd.DataFrame):
    """Event Hub'a temiz veriyi batch olarak gönderir"""
    if not all([EVENTHUB_NAMESPACE, EVENTHUB_NAME, EVENTHUB_KEY_NAME, EVENTHUB_SAS_KEY]):
        print("Eksik EventHub konfigürasyonu. Çıkılıyor.")
        return

    connection_str = f"Endpoint=sb://{EVENTHUB_NAMESPACE}.servicebus.windows.net/;SharedAccessKeyName={EVENTHUB_KEY_NAME};SharedAccessKey={EVENTHUB_SAS_KEY}"
    producer = EventHubProducerClient.from_connection_string(connection_str, eventhub_name=EVENTHUB_NAME)

    with producer:
        try:
            batch = producer.create_batch()
            for _, row in df.iterrows():
                event_body = row.to_json()
                try:
                    batch.add(EventData(event_body))
                except ValueError:
                    producer.send_batch(batch)
                    batch = producer.create_batch()
                    batch.add(EventData(event_body))
            if len(batch) > 0:
                producer.send_batch(batch)
            print(f"{len(df)} satır Event Hub'a gönderildi.")
        except Exception as e:
            print(f"EventHub gönderim hatası: {e}")


def stream_data():
    """CSV veriyi oku, temizle ve Event Hub'a gönder"""
    try:
        df = pd.read_csv(DATA_FILE)
        print(f"{len(df)} satır yüklendi.")
    except Exception as e:
        print(f"Dosya okunamadı: {e}")
        return

    df_clean = clean_dataframe(df)
    send_batch(df_clean)


if __name__ == "__main__":
    stream_data()
