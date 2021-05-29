import telnetlib, json, time
from datetime import datetime
import paho.mqtt.client as mqtt

mqttBroker = "169.254.217.163"
port = 1883

client = mqtt.Client("ST-10_2")
client.connect(mqttBroker, port)
while True:
    client.publish("HaasData", "This is ST-10_2", qos=0)
    time.sleep(1)