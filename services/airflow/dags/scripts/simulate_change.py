import os
import psycopg2
import time
import random


def simulate_change():
    time.sleep(5)
    print("\n🔄 Starting continuous data simulation for Housing data...\n", flush=True)

    config_postgres = {
        'dbname': os.getenv('POSTGRES_DB', 'baza_postgres'),
        'user': os.getenv('POSTGRES_USER', 'uzytkownik'),
        'password': os.getenv('POSTGRES_PASSWORD', 'haslo123'),
        'host': os.getenv('POSTGRES_HOST', 'localhost'),
        'port': os.getenv('POSTGRES_PORT', '5432')
    }

    conn = psycopg2.connect(**config_postgres)
    cur = conn.cursor()
    
    # Dostępne wartości dla Housing
    yes_no = ['yes', 'no']
    furnishing_statuses = ['furnished', 'semi-furnished', 'unfurnished']
    
    # Kolumny które mogą mieć NULL (wszystkie poza Index - kluczem głównym)
    nullable_columns = ['price', 'area', 'bedrooms', 'bathrooms', 'stories', 
                        'mainroad', 'guestroom', 'basement', 'hotwaterheating', 
                        'airconditioning', 'parking', 'prefarea', 'furnishingstatus']
    
    operation_count = 0
    
    try:
        while operation_count < 10:
            sleep_time = random.uniform(2, 6)
            time.sleep(sleep_time)
            
            operation = random.choice(['INSERT', 'UPDATE', 'DELETE'])
            
            operation_count += 1
            timestamp = time.strftime("%H:%M:%S")
            
            try:
                if operation == 'INSERT':
                    # Generuj losowe wartości dla nowego rekordu
                    price = random.randint(1500000, 15000000)
                    area = random.randint(1500, 16000)
                    bedrooms = random.randint(1, 6)
                    bathrooms = random.randint(1, 4)
                    stories = random.randint(1, 4)
                    mainroad = random.choice(yes_no)
                    guestroom = random.choice(yes_no)
                    basement = random.choice(yes_no)
                    hotwaterheating = random.choice(yes_no)
                    airconditioning = random.choice(yes_no)
                    parking = random.randint(0, 3)
                    prefarea = random.choice(yes_no)
                    furnishingstatus = random.choice(furnishing_statuses)
                    
                    # 20% szans na wstawienie NULL w losowej kolumnie (poza kluczem)
                    values = {
                        'price': price,
                        'area': area,
                        'bedrooms': bedrooms,
                        'bathrooms': bathrooms,
                        'stories': stories,
                        'mainroad': mainroad,
                        'guestroom': guestroom,
                        'basement': basement,
                        'hotwaterheating': hotwaterheating,
                        'airconditioning': airconditioning,
                        'parking': parking,
                        'prefarea': prefarea,
                        'furnishingstatus': furnishingstatus
                    }
                    
                    null_column = None
                    if random.random() < 0.2:  # 20% szans na NULL
                        null_column = random.choice(nullable_columns)
                        values[null_column] = None
                    
                    cur.execute("""
                        INSERT INTO housing 
                        ("Index", "price", "area", "bedrooms", "bathrooms", "stories", 
                         "mainroad", "guestroom", "basement", "hotwaterheating", 
                         "airconditioning", "parking", "prefarea", "furnishingstatus")
                        VALUES (DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING "Index"
                    """, (values['price'], values['area'], values['bedrooms'], values['bathrooms'], 
                          values['stories'], values['mainroad'], values['guestroom'], values['basement'],
                          values['hotwaterheating'], values['airconditioning'], values['parking'], 
                          values['prefarea'], values['furnishingstatus']))
                    
                    new_index = cur.fetchone()[0]
                    null_info = f" (NULL in {null_column})" if null_column else ""
                    print(f"[{timestamp}] ✅ Operation #{operation_count}: INSERTED new housing record (Index={new_index}, price={values['price']}, area={values['area']}){null_info}", flush=True)
                    
                elif operation == 'UPDATE':
                    cur.execute('SELECT "Index" FROM housing ORDER BY RANDOM() LIMIT 1')
                    result = cur.fetchone()
                    
                    if result:
                        index = result[0]
                        new_price = random.randint(1500000, 15000000)
                        new_bedrooms = random.randint(1, 6)
                        
                        # 15% szans na ustawienie NULL podczas UPDATE
                        if random.random() < 0.15:
                            update_col = random.choice(['price', 'bedrooms'])
                            if update_col == 'price':
                                new_price = None
                            else:
                                new_bedrooms = None
                        
                        cur.execute("""
                            UPDATE housing 
                            SET "price" = %s, "bedrooms" = %s
                            WHERE "Index" = %s
                        """, (new_price, new_bedrooms, index))
                        
                        print(f"[{timestamp}] 🔄 Operation #{operation_count}: UPDATED housing (Index={index}) - price: {new_price}, bedrooms: {new_bedrooms}", flush=True)
                    else:
                        print(f"[{timestamp}] ⚠️  Operation #{operation_count}: UPDATE skipped - no records found", flush=True)
                        
                elif operation == 'DELETE':
                    cur.execute('SELECT "Index", "price" FROM housing ORDER BY RANDOM() LIMIT 1')
                    result = cur.fetchone()
                    
                    if result:
                        index, price = result
                        cur.execute('DELETE FROM housing WHERE "Index" = %s', (index,))
                        print(f"[{timestamp}] ❌ Operation #{operation_count}: DELETED housing record (Index={index}, price={price})", flush=True)
                    else:
                        print(f"[{timestamp}] ⚠️  Operation #{operation_count}: DELETE skipped - no records found", flush=True)
                
                conn.commit()
                
            except Exception as op_error:
                print(f"[{timestamp}] ⚠️  Operation #{operation_count} failed: {op_error}", flush=True)
                conn.rollback()
                continue
            
    except KeyboardInterrupt:
        print(f"\n\n🛑 Simulation stopped. Total operations performed: {operation_count}", flush=True)
    except Exception as e:
        print(f"\n❗ Critical error occurred: {e}", flush=True)
        conn.rollback()
    finally:
        cur.close()
        conn.close()
        print("🔌 Database connection closed", flush=True)


if __name__ == '__main__':
    simulate_change()