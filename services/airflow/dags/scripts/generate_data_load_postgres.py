import pandas as pd
import os
import psycopg2

config_postgres = {
    'dbname': os.getenv('POSTGRES_DB', 'baza_postgres'),
    'user': os.getenv('POSTGRES_USER', 'uzytkownik'),
    'password': os.getenv('POSTGRES_PASSWORD', 'haslo123'),
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': os.getenv('POSTGRES_PORT', '5432')
}

# Ścieżka do pliku Housing.csv (relatywna do głównego katalogu projektu)
csv_file_path = os.getenv('CSV_FILE_PATH', '/opt/airflow/dags/scripts/data/Housing.csv')

def clean_database():
    """Usuwa wszystkie tabele z bazy danych"""
    connection = psycopg2.connect(**config_postgres)
    cursor = connection.cursor()
    
    print("🧹 Cleaning database...", flush=True)
    
    # Pobierz listę wszystkich tabel
    cursor.execute("""
        SELECT tablename 
        FROM pg_tables 
        WHERE schemaname = 'public'
    """)
    tables = cursor.fetchall()
    
    # Usuń każdą tabelę
    for table in tables:
        table_name = table[0]
        print(f"   Dropping table: {table_name}", flush=True)
        cursor.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE;')
    
    connection.commit()
    cursor.close()
    connection.close()
    print("✅ Database cleaned successfully!\n", flush=True)

def insert_files_postgress():
    connection = psycopg2.connect(**config_postgres)
    cursor = connection.cursor()

    print("📊 Inserting data from Housing.csv...", flush=True)
    
    # Wczytaj plik Housing.csv
    table = pd.read_csv(csv_file_path)
    table_name = "housing"
    
    # Dodaj kolumnę Index jako pierwszą kolumnę
    table.insert(0, 'Index', range(1, len(table) + 1))
    
    # Tworzenie definicji kolumn
    columns_definitions = []
    for col in table.columns:
        if col == 'Index':
            columns_definitions.append(f'"{col}" INTEGER NOT NULL')
        elif col in ['price', 'area', 'bedrooms', 'bathrooms', 'stories', 'parking']:
            columns_definitions.append(f'"{col}" INTEGER')
        else:
            columns_definitions.append(f'"{col}" TEXT')
    
    columns_sql = ", ".join(columns_definitions)
    create_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({columns_sql});'
    cursor.execute(create_sql)
    
    # Wstaw dane z CSV
    for row in table.itertuples(index=False, name=None):
        placeholders = ', '.join(['%s'] * len(row))
        columns = ', '.join([f'"{col}"' for col in table.columns])
        insert_sql = f'INSERT INTO "{table_name}" ({columns}) VALUES ({placeholders});'
        cursor.execute(insert_sql, row)
    
    # Po wstawieniu danych, znajdź maksymalną wartość Index
    cursor.execute(f'SELECT MAX("Index") FROM "{table_name}"')
    max_index = cursor.fetchone()[0]
    
    # Dodaj AUTO INCREMENT zaczynając od max_index + 1
    cursor.execute(f"""
        ALTER TABLE "{table_name}" 
        ALTER COLUMN "Index" ADD GENERATED ALWAYS AS IDENTITY (START WITH {max_index + 1});
    """)
    
    # Dodaj PRIMARY KEY
    cursor.execute(f'ALTER TABLE "{table_name}" ADD PRIMARY KEY ("Index");')
        
    print(f"   ✅ {len(table)} rows inserted into {table_name} (next ID will be {max_index + 1})", flush=True)
    
    connection.commit()
    cursor.close()
    connection.close()
    print("\n✅ All data inserted successfully!", flush=True)

if __name__ == '__main__':
    clean_database()
    insert_files_postgress()