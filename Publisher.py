import telnetlib, json
from datetime import datetime
import paho.mqtt.client as mqtt


msg1 = b"?Q100" + b'\n'  # SERIAL NUMBER
msg2 = b"?Q101" + b'\n'  # SOFTWARE VERSION
msg3 = b"?Q102" + b'\n'  # MODEL
msg4 = b"?Q600 3027" + b'\n'  # Spindle RPM
msg5 = b"?Q600 5021" + b'\n'  # Present Machine Coordinate Position X
msg6 = b"?Q600 5022" + b'\n'  # Present Machine Coordinate Position Y
msg7 = b"?Q600 5023" + b'\n'  # Present Machine Coordinate Position Z
message = [msg1, msg2, msg3, msg4, msg5, msg6, msg7]

# read data specific to setup and machines
with open("/home/pi/Haas-Data-Collection/Pub_config.txt") as config:
    # "/home/pi/Haas-Data-Collection/Pub_config.txt"
    mqttBroker = config.readline().split(" = ")[1].replace("\n","")
    client = config.readline().split(" = ")[1].replace("\n","")
    CNC_host = config.readline().split(" = ")[1].replace("\n","")

CNC_port = 5051
MQTT_port = 1883

client = mqtt.Client(client)
client.connect(mqttBroker, MQTT_port)

tn = telnetlib.Telnet(CNC_host, CNC_port)


while True:
    data ={}

    now = datetime.now()
    total_time = (now.hour * 3600) + (now.minute * 60) + (now.second)
    date = datetime.now().date()
    data["date"] = str(date)
    data["seconds"] = total_time

    # send all the Q codes
    for msg in message:
        tn.write(msg)

    # read and parse the message
    out = tn.read_until(msg,0.5).decode("utf-8").replace(">","").replace("\r\n","|").split("|")

    # create dictionary with all the data
    for n, msg in enumerate(out):
        msg = msg.split(", ")
        if n == 3:
            data["RPM"] = float(msg[1])
        elif n == 4:
            data["X"] = float(msg[1])
        elif n == 5:
            data["Y"] = float(msg[1])
        elif n == 6:
            data["Z"] = float(msg[1])
        elif len(msg)<=1:
            pass
        else:
            data[msg[0]] = msg[1]

    jsondata = json.dumps(data)
    client.publish(client, jsondata, qos=0)

    print("Data published in topic {}".format(client), data)