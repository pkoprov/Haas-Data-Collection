import time

import pandas as pd
import psycopg2 as pg
from psycopg2.extras import execute_values


def update(con, data):
    cur = con.cursor()
    execute_values(cur, """UPDATE "AML"."ST-10_2" 
                           SET "timestamp" = update_payload.timestamp 
                           FROM (VALUES %s) AS update_payload (date,time, timestamp) 
                           WHERE "Year, month, day" = update_payload.date AND
                            "Hour, minute, second" = update_payload.time""", data)
    con.commit()


table = "ST-10_2"

with open(r"C:\Users\pkoprov\PycharmProjects\Haas-Data-Collection\historian.config",
          'r') as config:  # uncomment for Windows
    mqttBroker = config.readline().split(" = ")[1].replace("\n", "")
    myGroupId = config.readline().split(" = ")[1].replace("\n", "")
    dbName = config.readline().split(" = ")[1].replace("\n", "")
    myUsername = config.readline().split(" = ")[1].replace("\n", "")
    myPassword = config.readline().split(" = ")[1].replace("\n", "")

conn = pg.connect(f"dbname={dbName} user={myUsername} password={myPassword}")  # connect to DB

try:
    cur = conn.cursor()  # create a cursor object
    print("Subscriber connection established")
except (Exception, pg.DatabaseError) as error:
    print(error)

dbData = pd.read_sql_query(f'SELECT * FROM "AML"."{table}" where "timestamp" is null', conn)

date = (pd.to_datetime(dbData['Year, month, day']) + pd.Timedelta("4 hours")).values.astype(float) / 10e8
tim = pd.to_timedelta(dbData['Hour, minute, second']).values.astype(float) / 10e8
dat_tim = date + tim

print(dbData.shape[0], "rows to update")

data = []
for date, tim, stamp in zip(dbData['Year, month, day'], dbData['Hour, minute, second'], dat_tim):
    data.append((date, tim, stamp))

start = time.perf_counter()

update(conn, data)
print(time.perf_counter() - start)
