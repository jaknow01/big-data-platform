import pandas as pd
import os
import psycopg2
from simulate_change import simulate_change

config_postgres = {
    'dbname': 'baza_postgres',
    'user': 'uzytkownik',
    'password': 'haslo123',
    'host': 'localhost',
    'port': 5432
}

csv_path = "csv_files"
csv_files_list = os.listdir(csv_path)

def insert_files_postgress():
    connection = psycopg2.connect(**config_postgres)
    cursor = connection.cursor()

    for file in csv_files_list:
        file_path = os.path.join(csv_path, file)
        table = pd.read_csv(file_path)
        table_name = file.split(".")[0]
        
        # Tworzenie definicji kolumn z kluczem głównym na kolumnie Index
        columns_definitions = []
        for col in table.columns:
            if col.lower() == 'index':
                columns_definitions.append(f'"{col}" SERIAL PRIMARY KEY')
            else:
                columns_definitions.append(f'"{col}" TEXT')
        
        columns_sql = ", ".join(columns_definitions)
        create_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({columns_sql});'
        cursor.execute(create_sql)
        
        for row in table.itertuples(index=False, name=None):
            placeholders = ', '.join(['%s'] * len(row))
            insert_sql = f'INSERT INTO "{table_name}" VALUES ({placeholders});'
            cursor.execute(insert_sql, row)
            
        print(f"{len(table)} rows inserted into {table_name}")
    
    connection.commit()
    cursor.close()
    connection.close()

if __name__ == '__main__':
    insert_files_postgress()
    simulate_change()
    
