import os
import psycopg2
import time
import random


def simulate_change():
    time.sleep(5)
    print("\n🔄 Starting continuous data simulation...\n")

    config_postgres = {
        'dbname': 'baza_postgres',
        'user': 'uzytkownik',
        'password': 'haslo123',
        'host': 'localhost',
        'port': 5432
    }

    conn = psycopg2.connect(**config_postgres)
    cur = conn.cursor()
    
    countries = ['Poland', 'USA', 'Germany', 'France', 'Spain', 'Italy', 'Japan', 'Brazil']
    industries = ['Technology', 'Finance', 'Healthcare', 'Education', 'Manufacturing', 'Retail']
    
    operation_count = 0
    
    try:
        while True:
            sleep_time = random.uniform(2, 6)
            time.sleep(sleep_time)
            
            # Losowy wybór operacji: 1=INSERT, 2=UPDATE, 3=DELETE
            operation = random.choice(['INSERT', 'UPDATE', 'DELETE'])
            
            operation_count += 1
            timestamp = time.strftime("%H:%M:%S")
            
            if operation == 'INSERT':
                org_id = ''.join(random.choices('0123456789ABCDEFabcdef', k=15))
                name = f"Company {random.randint(1000, 9999)}"
                website = f"https://company{random.randint(100, 999)}.com"
                country = random.choice(countries)
                description = "Auto-generated test company"
                founded = random.randint(1970, 2023)
                industry = random.choice(industries)
                employees = random.randint(100, 10000)
                
                cur.execute("""
                    INSERT INTO organizations_100 
                    ("Organization Id", "Name", "Website", "Country", "Description", "Founded", "Industry", "Number of employees")
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (org_id, name, website, country, description, founded, industry, employees))
                
                print(f"[{timestamp}] ✅ Operation #{operation_count}: INSERTED new company '{name}' from {country}")
                
            elif operation == 'UPDATE':
                cur.execute('SELECT "Index" FROM organizations_100 ORDER BY RANDOM() LIMIT 1')
                result = cur.fetchone()
                
                if result:
                    index = result[0]
                    new_employees = random.randint(100, 10000)
                    new_country = random.choice(countries)
                    
                    cur.execute("""
                        UPDATE organizations_100 
                        SET "Number of employees" = %s, "Country" = %s
                        WHERE "Index" = %s
                    """, (new_employees, new_country, index))
                    
                    print(f"[{timestamp}] 🔄 Operation #{operation_count}: UPDATED organization (Index={index}) - employees: {new_employees}, country: {new_country}")
                else:
                    print(f"[{timestamp}] ⚠️  Operation #{operation_count}: UPDATE skipped - no records found")
                    
            elif operation == 'DELETE':
                cur.execute('SELECT "Index", "Name" FROM organizations_100 ORDER BY RANDOM() LIMIT 1')
                result = cur.fetchone()
                
                if result:
                    index, name = result
                    cur.execute('DELETE FROM organizations_100 WHERE "Index" = %s', (index,))
                    print(f"[{timestamp}] ❌ Operation #{operation_count}: DELETED organization '{name}' (Index={index})")
                else:
                    print(f"[{timestamp}] ⚠️  Operation #{operation_count}: DELETE skipped - no records found")
            
            conn.commit()
            
    except KeyboardInterrupt:
        print(f"\n\n🛑 Simulation stopped. Total operations performed: {operation_count}")
    except Exception as e:
        print(f"\n❗ Error occurred: {e}")
    finally:
        cur.close()
        conn.close()
        print("🔌 Database connection closed")


if __name__ == '__main__':
    simulate_change()