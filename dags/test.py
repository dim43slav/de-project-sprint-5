import psycopg2

try:
    conn = psycopg2.connect(
        host="localhost",
        database="de",
        user="jovyan",
        password="jovyan",
        port=15432
    )
    print("Подключение успешно!")
    conn.close()
except Exception as e:
    print(f"Ошибка: {e}")