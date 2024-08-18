import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

def load_env():
    """
    Memuat variabel lingkungan dari file .env di folder yang sama
    """
    env_path = os.path.join(os.path.dirname(__file__), '.env')
    load_dotenv(dotenv_path=env_path)

def postgres_engine_sales_data():
    """
    Helper function untuk melakukan koneksi antara Pandas
    dengan PostgreSQL. Sesuaikan username, password,
    host, dan database name dengan milik masing - masing
    """
    # Memuat file .env dari folder yang sama
    load_env()

    # Mengambil variabel lingkungan
    DB_USERNAME = os.getenv("DB_USERNAME")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_HOST = os.getenv("DB_HOST")
    DB_NAME = os.getenv("DB_NAME")
    DB_PORT = os.getenv("DB_PORT")

    # Membuat string koneksi
    connection_string = f"postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(connection_string)

    return engine


# engine_sales_data = postgres_engine_sales_data()