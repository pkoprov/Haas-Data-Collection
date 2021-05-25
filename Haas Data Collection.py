import re, time, telnetlib

#change the IP address if needed
HOST={'VF1_1':['192.168.1.20'],'VF1_2':['192.168.1.21'],'ST10_1':['192.168.1.22'],'ST10_2':['192.168.1.23']}
PORT=5051
msg1 = b"?Q100" + b'\n'  # SERIAL NUMBER
msg2 = b"?Q101" + b'\n'  # SOFTWARE VERSION
msg3 = b"?Q102" + b'\n'  # MODEL
msg4 = b"?Q600 3027" + b'\n'  # Spindle RPM
msg5 = b"?Q600 5021" + b'\n'  # Present Machine Coordinate Position X
msg6 = b"?Q600 5022" + b'\n'  # Present Machine Coordinate Position Y
msg7 = b"?Q600 5023" + b'\n'  # Present Machine Coordinate Position Z
message = [msg1, msg2, msg3, msg4, msg5, msg6, msg7]

while True:

    for i in HOST:
        print(i,HOST[i])
        tn = telnetlib.Telnet(HOST[i][0], PORT)
        for msg in message:
            tn.write(msg)
        out = tn.read_until(msg,0.5)
        with open("Haas Data Collection.csv", 'ab') as file:
            file.write(out)
        print(out)
    if 0XFF == ord('q'):
        break
x = out.decode("utf-8").split(">>")
for msg in x:
    msg = msg.replace("\r\n", '')
    print(msg)
    if "SERIAL" in msg:
        value = msg[1]
        # value = re.findall("\d+",x[1])
        print("Serial", value)
    elif "SOFTWARE" in msg:
        value = re.findall("\d+",msg)
        value = '.'.join(value)
        print("Soft_v", value)
    elif "MODEL" in msg:
        value = re.findall("\d+", x[1])




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
# msg12 = b"?Q600 1067"+b'\n' #Maximum Load for Axis A
# msg13 = b"?Q600 1068"+b'\n' #Maximum Load for Axis B
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
