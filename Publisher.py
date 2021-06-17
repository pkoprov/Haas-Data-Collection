import json
import telnetlib
import time

import paho.mqtt.client as mqtt
import pandas as pd


def parse(telnetdata):
    data_dict = {}
    for n, msg in enumerate(telnetdata):
        val_list = msg.split(", ")  # split message into variable and value
        if len(val_list) == 1 or len(val_list) == 5:
            data_dict['Three-in-one (PROGRAM, Oxxxxx, STATUS, PARTS, xxxxx)'] = msg
            # print(msg)
        elif val_list[1] != "?":  # if value exists
            var = Q_codes["Description"][n]
            data_dict[var] = val_list[1]
    return data_dict


def publish(data_dict):
    jsondata = json.dumps(data_dict)
    client.publish(topic, jsondata, qos=0)
    print(f"Data published in topic {topic} {data_dict}")


# /home/pi/Haas-Data-Collection/
Q_codes = pd.read_excel("/home/pi/Haas-Data-Collection/DB Table columns.xlsx", sheet_name="Static")
Q_codes = Q_codes.append(pd.read_excel("/home/pi/Haas-Data-Collection/DB Table columns.xlsx", sheet_name="Variable"),
                         ignore_index=True)

# read data specific to setup and machines
with open("/home/pi/Haas-Data-Collection/Pub_config.txt") as config:
    # "/home/pi/Haas-Data-Collection/Pub_config.txt"
    mqttBroker = config.readline().split(" = ")[1].replace("\n", "")
    client = config.readline().split(" = ")[1].replace("\n", "")
    CNC_host = config.readline().split(" = ")[1].replace("\n", "")

topic = client
# topic = "HaasData"
CNC_port = 5051
MQTT_port = 1883

client = mqtt.Client(client)
client.connect(mqttBroker, MQTT_port)

tn = telnetlib.Telnet(CNC_host, CNC_port, 1)

last_out = []
omit = list(Q_codes[(Q_codes['Variable'] == '?Q600 3012') + (Q_codes['Variable'] == '?Q300') + (
        Q_codes['Variable'] == '?Q600 3020')].index)

while True:

    # transform Q-codes from table to binary and send to the CNC machine
    for i in Q_codes["Variable"]:
        msg = i.encode("ascii") + b"\n"
        tn.write(msg)

    out = tn.read_until(msg, 1).decode("utf-8").replace(">", '').replace("\r\n", "|").split("|")
    out.pop(-1)

    if last_out:
        new_out = out[:omit[0]] + out[omit[0] + 1:omit[1]] + out[omit[1] + 1:omit[2]] + out[omit[2] + 1:]
        if new_out == last_out:
            print("pass")
            time.sleep(1)
            pass
        else:
            data = parse(out)
            publish(data)
            last_out = out[:omit[0]] + out[omit[0] + 1:omit[1]] + out[omit[1] + 1:omit[2]] + out[omit[2] + 1:]
            time.sleep(1)
    else:
        print('empty')
        data = parse(out)
        publish(data)
        last_out = out[:omit[0]] + out[omit[0] + 1:omit[1]] + out[omit[1] + 1:omit[2]] + out[omit[2] + 1:]
        time.sleep(1)
