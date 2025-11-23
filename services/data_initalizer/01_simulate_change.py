import os
import psycopg2
from task1 import config_postgres


def simulate_change():
    import time
    time.sleep(5)  # daj Debezium czas na rozruch
    print("\n🔄 Simulating data change...")

    conn = psycopg2.connect(**config_postgres)
    cur = conn.cursor()
    
    cur.execute("DELETE FROM organizations_100 WHERE \"Country\" = 'China';")
    conn.commit()

    print("✅ Deleted chinese companies")
    cur.close()
    conn.close()

if __name__ == '__main__':
    simulate_change()