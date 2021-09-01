import json

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

topic_list = ['VF-2_1','VF-2_2' ]

while True:
    dataObj ={}
    try:
        for n, topic in enumerate(topic_list):
            CMD = f'SELECT * FROM public."{topic}" order by "Year, month, day" desc, "Power-on Time (total)" desc limit 1'
            df = pd.read_sql_query(CMD, conn)
            dataObj[topic] = df.to_dict(orient='records')


        message = json.dumps(dataObj)

        client.publish(f'FWH2200_PG_DB/output',message, qos=0)
        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")
        print(f"Message is sent at {current_time}")
        time.sleep(1)
    except:
        print('Something went wrong')