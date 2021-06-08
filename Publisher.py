import telnetlib, json, time
import paho.mqtt.client as mqtt
import pandas as pd

Q_codes = pd.read_excel("./Haas-Data-Collection/Q-codes.xlsx", sheet_name="Global")
Q_codes = Q_codes.append(pd.read_excel("./Haas-Data-Collection/Q-codes.xlsx",sheet_name="Macros"), ignore_index = True)


# read data specific to setup and machines
with open("/home/pi/Haas-Data-Collection/Pub_config.txt") as config:
    # "/home/pi/Haas-Data-Collection/Pub_config.txt"
    mqttBroker = config.readline().split(" = ")[1].replace("\n","")
    client = config.readline().split(" = ")[1].replace("\n","")
    CNC_host = config.readline().split(" = ")[1].replace("\n","")

topic = client
# topic = "HaasData"
CNC_port = 5051
MQTT_port = 1883

client = mqtt.Client(client)
client.connect(mqttBroker, MQTT_port)

tn = telnetlib.Telnet(CNC_host, CNC_port)


while True:
    data = {}

    for n, i in enumerate(Q_codes["Variable"]):
        msg = i.encode("ascii") + b"\n"
        tn.write(msg)

    out = tn.read_until(msg, 0.5).decode("utf-8").replace(">", '').replace("\r\n", "|").split("|")
    out.pop(-1)

    for n, value in enumerate(out):
        val_list = value.split(", ")
        if value[1] != "?":
            data[Q_codes["Description"][n]] = val_list[1]

    jsondata = json.dumps(data)
    client.publish(topic, jsondata, qos=0)

    print("Data published in topic {}".format(topic))
    time.sleep(1)