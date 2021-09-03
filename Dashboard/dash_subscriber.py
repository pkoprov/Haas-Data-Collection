import paho.mqtt.client as mqtt
import json


with open("dash_pub_config.txt") as config:
    mqttBroker = config.readline().split(" = ")[1].replace("\n", "")


def on_connect(client, userdata, flags, rc):
    if rc==0:
        print("connected OK Returned code=",rc)
    else:
        print("Bad connection Returned code=",rc)


def on_log(client, userdata, level, buffer):
    print("Log: ", buffer)


def on_message(client, userdata, message):
    msg = message.payload.decode("utf-8")
    print("Received message in topic", message.topic)
    # global dataObj
    # dataObj = json.loads(msg)
    print(msg)

port=1883
client = mqtt.Client("pkoprov")
client.connect(mqttBroker, port)
client.on_connect = on_connect
# client.on_log = on_log
client.on_message = on_message

client.subscribe('FWH2200_PG_DB/#')
# client.loop_forever()
client.loop_start()
while True:
    pass
# client.loop_stop()
