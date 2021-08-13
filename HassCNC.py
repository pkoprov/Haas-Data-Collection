import json

import paho.mqtt.client as mqtt
import time
import pandas as pd
import openpyxl as xl

def on_message(client, userdata, message):
    msg = message.payload.decode("utf-8")
    print("Received MQTT message: ", message)
    global x
    x = message.payload



mqttBroker = 'mqtt.eclipseprojects.io'
topic = 'FWH2200_PG_DB/output'
client = mqtt.Client('receiver')
client.connect(mqttBroker)

client.subscribe(topic)
client.on_message = on_message
client.loop_start()
time.sleep(1)
client.loop_stop()
# client.loop_forever()
type(json.loads(x))
