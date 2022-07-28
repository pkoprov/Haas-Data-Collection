import json
import sys

import paho.mqtt.client as mqtt

sys.path.insert(0, r'C:\Users\pkoprov\PycharmProjects\Haas-Data-Collection\spb')  # uncomment for Windows

from sparkplug_b import *

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        client.subscribe('spBv1.0/FWH2200/DDATA/RPI-VF-2-1/VF-2_1', qos)
        print("Connected with result code " + str(rc))
    else:
        print("Failed to connect with result code " + str(rc))
        sys.exit()


def on_message(client, userdata, msg):
    global inboundPayload
    print("Message arrived: " + msg.topic)
    global inboundPayload
    if msg.topic == 'spBv1.0/FWH2200/DDATA/RPI-VF-2-1/VF-2_1' :  # check if the message is for this topic
        inboundPayload = sparkplug_b_pb2.Payload()  # create a payload object
        inboundPayload.ParseFromString(msg.payload)  # parse the payload into the payload object
        msg = {}
        for met in inboundPayload.metrics:
            if "Present machine coordinate position" in met.name:
                msg[met.name] = round(met.float_value, 2)

        print(msg)
        client.publish("json/FWH2200/DDATA/DT/VF-2_1", json.dumps(msg), 2)


# setup MQTT client, callbacks and connection
qos = 2
client = mqtt.Client("DT_client", True)
client.on_connect = on_connect
client.on_message = on_message
client.connect("192.168.10.2", 1883, 60)

while True:
    client.loop()
