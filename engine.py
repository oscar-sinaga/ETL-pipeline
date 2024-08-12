import os
from sqlalchemy import create_engine, text

# Fungsi untuk memuat file .env secara manual
def load_env(filepath):
    with open(filepath) as f:
        for line in f:
            if line.strip() and not line.startswith("#"):
                key, value = line.strip().split('=', 1)
                os.environ[key] = value

# Memuat file .env
load_env('.env')

# Mengambil variabel lingkungan
DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_PORT = os.getenv("DB_PORT")

# Membuat string koneksi
connection_string = f"postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(connection_string)

# Mencoba melakukan koneksi untuk memastikan koneksi berhasil
# Menggunakan `text` untuk menjalankan SQL
# Mencoba melakukan koneksi untuk memastikan koneksi berhasil
try:
    with engine.connect() as connection:
        result = connection.execute(text("SELECT * from amazon_sales_data limit 3;"))
        print("Koneksi berhasil:", result.fetchone())
except Exception as e:
    print("Koneksi gagal:", e)