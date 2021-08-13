import psycopg2 as pg
import paho.mqtt.client as mqtt
import pandas as pd
import time
from datetime import datetime


with open("dash_pub_config.txt") as config:
    mqttBroker = config.readline().split(" = ")[1].replace("\n", "")
    db = config.readline().split(" = ")[1].replace("\n", "")
    user = config.readline().split(" = ")[1].replace("\n", "")
    password = config.readline().split(" = ")[1].replace("\n", "")

conn = pg.connect(f"dbname={db} user={user} password={password}")


try:
    cur = conn.cursor()
    print("Subscriber connection established")
except (Exception, pg.DatabaseError) as error:
    print(error)


port=1883
client = mqtt.Client("FWH2200_PG_DB")
client.connect(mqttBroker, port)

while True:
    try:
        topic = 'VF-2_1'
        CMD = f'SELECT * FROM public."{topic}" order by "Power-on Time (total)" desc limit 1'
        df = pd.read_sql_query(CMD, conn)
        message = df.to_json(orient='records')

        client.publish(f'FWH2200_PG_DB/output',message, qos=0)
        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")
        print(f"Message is sent at {current_time}")
        time.sleep(1)
    except:
        print('Something went wrong')