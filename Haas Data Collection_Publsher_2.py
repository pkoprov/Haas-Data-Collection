import telnetlib, json
from datetime import datetime
import paho.mqtt.client as mqtt

mqttBroker = "broker.hivemq.com"

client = mqtt.Client("Machine1")
client.connect(mqttBroker)

#change the IP address if needed
HOST='192.168.1.20'
PORT=5051
tn = telnetlib.Telnet(HOST, PORT)

msg1 = b"?Q100" + b'\n'  # SERIAL NUMBER
msg2 = b"?Q101" + b'\n'  # SOFTWARE VERSION
msg3 = b"?Q102" + b'\n'  # MODEL
msg4 = b"?Q600 3027" + b'\n'  # Spindle RPM
msg5 = b"?Q600 5021" + b'\n'  # Present Machine Coordinate Position X
msg6 = b"?Q600 5022" + b'\n'  # Present Machine Coordinate Position Y
msg7 = b"?Q600 5023" + b'\n'  # Present Machine Coordinate Position Z
message = [msg1, msg2, msg3, msg4, msg5, msg6, msg7]

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
    client.publish("Haas/VF-2_1/Data", jsondata, qos=0)

    print("Data published..,", data)





# #msg0 = b"?Start of Next Run"+b"\n\"
# msg1 = b"?Q100"+b'\n'
# msg2 = b"?Q101"+b'\n'
# msg3 = b"?Q102"+b'\n'
# msg4 = b"?Q104"+b'\n'
# msg5 = b"?Q200"+b'\n'
# msg6 = b"?Q201"+b'\n'
# msg7 = b"?Q300"+b'\n' #Power On Time
# msg8 = b"?Q301"+b'\n' #Motion Time
# #msg9 = b"?Q600 1064"+b'\n' #Maximum Load for Axis X
# #msg10 = b"?Q600 1065"+b'\n' #Maximum Load for Axis Y
# #msg11 = b"?Q600 1066"+b'\n' #Maximum Load for Axis Z
# #msg12 = b"?Q600 1067"+b'\n'
# #msg13 = b"?Q600 1068"+b'\n'
# msg14 = b"?Q600 3026"+b'\n' #Tool in Spindle
# msg15 = b"?Q600 3027"+b'\n' #Spindle RPM
# msg16 = b"?Q600 1098"+b'\n' #Spindle Load
# msg17 = b"?Q600 3011"+b'\n' #Year, month, day
# msg18 = b"?Q600 3012"+b'\n' #Hour, minute, second
# msg19 = b"?Q600 5021"+b'\n' #Present Machine Coordinate Position X
# msg20 = b"?Q600 5022"+b'\n' #Present Machine Coordinate Position Y
# msg21 = b"?Q600 5023"+b'\n' #Present Machine Coordinate Position Z
# #msg22 = b"?Q600 4001"+b'\n' #Previous G-Block Group 1
# #msg23 = b"?Q600 4002"+b'\n' #Previous G-Block Group 2
# #msg24 = b"?Q600 4003"+b'\n' #Previous G-Block Group 3
# msg25 = b"?Q600 8552"+b'\n' #Maximum Recorded Vibrations
# msg26 = b"?Q600 13013"+b'\n' #Coolant Level
