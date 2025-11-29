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

csv_path = "data/"
csv_files_list = os.listdir(csv_path)

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

    print("📊 Inserting data from CSV files...", flush=True)
    
    for file in csv_files_list:
        if not file.endswith('.csv'):
            continue
            
        file_path = os.path.join(csv_path, file)
        table = pd.read_csv(file_path)
        table_name = file.split(".")[0]
        
        # Tworzenie definicji kolumn (Index jako INTEGER NOT NULL, reszta TEXT)
        columns_definitions = []
        for col in table.columns:
            if col.lower() == 'index':
                columns_definitions.append(f'"{col}" INTEGER NOT NULL')
            else:
                columns_definitions.append(f'"{col}" TEXT')
        
        columns_sql = ", ".join(columns_definitions)
        create_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({columns_sql});'
        cursor.execute(create_sql)
        
        # Wstaw dane z CSV (włącznie z kolumną Index)
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