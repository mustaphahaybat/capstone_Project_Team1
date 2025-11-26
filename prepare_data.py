import pandas as pd
import os

# Dosya yolları: Kök dizine göre 'data/' klasörüne ulaşıyoruz
BASE_DIR = os.path.join(os.getcwd(), 'data')
INPUT_FILE = os.path.join(BASE_DIR, 'booking_dirty.csv')
BATCH_FILE = os.path.join(BASE_DIR, 'hotel_raw_batch.csv')
STREAM_FILE = os.path.join(BASE_DIR, 'hotel_raw_stream.csv')

# --- ÖN KONTROL ---
# İndirilen dosyanın mevcut olduğunu kontrol eder.
if not os.path.exists(INPUT_FILE):
    print(f"HATA: '{INPUT_FILE}' bulunamadı. Lütfen indirdiğiniz dosyayı 'data/' klasörüne taşıyın.")
else:
    print(f"'{INPUT_FILE}' okunuyor...")
    df = pd.read_csv(INPUT_FILE)

    # Veriyi karıştırma ve bölme: %70 Batch, %30 Stream
    df = df.sample(frac=1, random_state=42).reset_index(drop=True)

    total_rows = len(df)
    split_point = int(total_rows * 0.70)

    batch_df = df.iloc[:split_point]
    stream_df = df.iloc[split_point:]

    # İki ayrı dosyaya kaydetme
    batch_df.to_csv(BATCH_FILE, index=False)
    stream_df.to_csv(STREAM_FILE, index=False)

    print(f"Batch verisi kaydedildi: {len(batch_df)} satır")
    print(f"Stream verisi kaydedildi: {len(stream_df)} satır")
    print("Veri hazırlığı tamamlandı.")