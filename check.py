import sqlite3

conn = sqlite3.connect("weather.db")
cursor = conn.cursor()

print("\n--- INDEXES ---")
cursor.execute("PRAGMA index_list(weather_data)")
print(cursor.fetchall())

for idx in cursor.fetchall():
    name = idx[1]
    print(f"\nIndex: {name}")
    cursor.execute(f"PRAGMA index_info({name})")
    print(cursor.fetchall())

conn.close()