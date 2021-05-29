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
while True:
    client.publish("HaasData", "This is ST-10_2", qos=0)
    time.sleep(1)