from config import DATABASE_URL
import psycopg2

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

# Diğer veritabanı işlemleri gerekirse buraya eklenebilir