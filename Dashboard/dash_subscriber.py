import paho.mqtt.client as mqtt
import json


with open("dash_pub_config.txt") as config:
    mqttBroker = config.readline().split(" = ")[1].replace("\n", "")


def on_connect(client, userdata, flags, rc):
    print("Connected with Result Code: {}".format(rc))


# def on_log(client, userdata, level, buffer):
#     print("Log: ", buffer)


def on_message(client, userdata, message):
    msg = message.payload.decode("utf-8")
    print("Received message in topic", message.topic)
    global dataObj
    dataObj = json.loads(msg)
    print(msg)

port=1883
client = mqtt.Client("pkoprov")
client.connect(mqttBroker, port)
client.on_connect = on_connect
# client.on_log = on_log
client.on_message = on_message

client.subscribe(f'FWH2200_PG_DB/output')
client.loop_forever()
# client.loop_start()
# client.loop_stop()