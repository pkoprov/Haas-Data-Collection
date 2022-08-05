import pandas as pd
import psycopg2 as pg

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


dbData = pd.read_sql_query(f'SELECT * FROM "AML"."{table}"',   conn)

date = (pd.to_datetime(dbData['Year, month, day']) + pd.Timedelta("4 hours")).values.astype(float)/10e8
tim = pd.to_timedelta(dbData['Hour, minute, second']).values.astype(float)/10e8
dat_tim = date + tim


CMD = ''
for date, time, stamp in zip(dbData['Year, month, day'], dbData['Hour, minute, second'], dat_tim):
    CMD = f'''UPDATE "AML"."{table}" SET "timestamp"= {stamp} where "Year, month, day" = '{date}' and "Hour, minute, second" = '{time}'; '''
    cur.execute(CMD)

conn.commit()
