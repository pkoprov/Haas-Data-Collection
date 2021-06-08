import telnetlib, json, time
import paho.mqtt.client as mqtt
import pandas as pd

Q_codes = pd.read_excel("./Haas-Data-Collection/Global Q-codes.xlsx")
Q_codes = Q_codes.append(pd.read_excel("./Haas-Data-Collection/Q-codes.xlsx"), ignore_index = True)


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
    # dictionary for the future read from telnet
    data = {}

    # transform Q-codes from table to binary and send to the CNC machine
    for n, i in enumerate(Q_codes["Variable"]):
        msg = i.encode("ascii") + b"\n"
        tn.write(msg)

    # read data from CNC and parse it
    out = tn.read_until(msg, 0.5).decode("utf-8").replace(">", '').replace("\r\n", "|").split("|")
    out.pop(-1)

    # fill the dictionary with data
    for n, msg in enumerate(out):
        val_list = msg.split(", ")  # split message into topic and value
        if n == 12:
            data["Status"] = msg
            # print(msg)
        elif val_list[1] != "?": # if value exists
            var = Q_codes["Description"][n]
            data[var] = val_list[1]
            # print(var, val_list[1])

    jsondata = json.dumps(data)
    client.publish(topic, jsondata, qos=0)

    print(f"Data published in topic {topic} {data}")
    time.sleep(1)

