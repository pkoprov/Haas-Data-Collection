import telnetlib, json, time
from datetime import datetime
import paho.mqtt.client as mqtt

with open("Haas-Data-Collection/Pub_config.txt") as config:
    mqttBroker = config.readline()
    client = config.readline()

mqttBroker = mqttBroker.replace("\n","")
port = 1883

client = mqtt.Client(client)
client.connect(mqttBroker, port)
msg = ("This is {}".format(client))
while True:
    client.publish("HaasData", msg, qos=0)
    print("Just published", datetime.now().strftime("%H:%M:%S"))
    time.sleep(1)