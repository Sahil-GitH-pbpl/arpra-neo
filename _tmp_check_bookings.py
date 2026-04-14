import pymysql
conn=pymysql.connect(host='localhost',user='root',password='',database='lead_management',cursorclass=pymysql.cursors.DictCursor)
try:
    with conn.cursor() as cur:
        cur.execute("SELECT id, booking_code, caller_id, booking_status, assigned_phlebotomist_id, created_at FROM hhome_collection_booking ORDER BY id")
        rows=cur.fetchall()
        print('booking_count', len(rows))
        for r in rows:
            print(r)

        cur.execute("SELECT AUTO_INCREMENT FROM information_schema.tables WHERE table_schema='lead_management' AND table_name='hhome_collection_booking'")
        print('next_ai', cur.fetchone()['AUTO_INCREMENT'])
finally:
    conn.close()
