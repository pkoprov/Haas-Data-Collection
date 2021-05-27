import json
from datetime import datetime
import time
import psycopg2 as pg
import os

import paho.mqtt.client as mqtt

# conn = pg.connect("postgres://dtgybdjldniags:578128895c05b6be9aa3d44a3c644d672317f9c0d19c631ab4a046cfda839d73@ec2-52-204-141-94.compute-1.amazonaws.com:5432/d6v91jpejet3nc",sslmode="require")
# # conn = pg.connect(os.environ.get("DATABASE_URL"), sslmode="require")
#
# try:
#     cur = conn.cursor()
#     print("Subscriber connection established")
# except (Exception, pg.DatabaseError) as error:
#     print(error)


def on_connect(client, userdata, flags, rc):
    print("Connected with Result Code: {}".format(rc))

def on_log(client, userdata, level, buffer):
    print("Log: ", buffer)

def on_disconnect(client, userdata, rc=0):
    client.loop_stop()

def on_message(client, userdata, message):
    print("Received message:", message.topic)
    msg = message.payload.decode("utf-8")
    dataObj=json.loads(msg)
    print(dataObj)

    # machine_id = 1002
    # date = dataObj["date"]
    # timestamp = dataObj["seconds"]
    # temperature = dataObj["Temperature"]
    # pressure = dataObj["Pressure"]
    # humidity = dataObj["Humidity"]
    #
    # insertQ = """ INSERT INTO public."sensorData" ("machine_id", "timestamp", "date", "pressure", "temperature", "humidity")
    #                     VALUES(%s,%s,%s,%s,%s,%s)"""
    # record = (machine_id, timestamp, date, pressure, temperature, humidity)

    try:
        # cur.execute(insertQ, record)
        print("DB Transaction executed")

        # commit all transactions after the loop has stopped.
        # conn.commit()

    except (Exception, pg.DatabaseError) as error:
        print(error)

#MAIN
mqttBroker = "localhost"
port = 1883

client = mqtt.Client("Subscriber")
client.connect(mqttBroker, port)

#call-back functions
client.on_connect = on_connect
client.on_log = on_log
client.on_message = on_message


client.loop_start()

client.subscribe("Haas/VF-2_1/Data")
time.sleep(10)

client.loop_stop()
