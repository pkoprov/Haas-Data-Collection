import json, telnetlib, time
import paho.mqtt.client as mqtt


def on_connect(client, userdata, flags, rc):
    client.publish(f"spBv1.0/FWH2200/NBIRTH/{asset}_RPI", 'ONLINE', retain=True)
    print("Connected with Result Code: {}".format(rc))


def on_disconnect(client, userdata, flags, rc):
    client.publish(f"spBv1.0/FWH2200/NDEATH/{asset}_RPI", 'OFFLINE', retain=True)
    print("Disconnected with Result Code: {}".format(rc))


# function to parse output of CNC machine from telnet port
def parse(telnetdata):
    data_dict = {}
    for n, msg in enumerate(telnetdata):
        val_list = msg.split(", ")  # split message into variable and value
        if len(val_list) == 1 or len(val_list) == 5:
            data_dict['Three-in-one (PROGRAM, Oxxxxx, STATUS, PARTS, xxxxx)'] = msg
            # print(msg)
        elif val_list[1] != "?":  # if value exists
            var = Description[n]
            data_dict[var] = val_list[1]
    return data_dict


def publish_device_data(telnetdata):
    raw_data = parse(telnetdata)
    jsondata = json.dumps(raw_data)

    data_topic = f"spBv1.0/FWH2200/DDATA/{asset}_RPI/{asset}"

    client.publish(data_topic, jsondata, qos=2)
    print(f"Data published in topic {data_topic} {raw_data}")


def telnet_connection(fail_message):
    global tn, telnet_status, tn_err_msg
    try:
        client.publish(f"ping/{topic}", '')
        tn = telnetlib.Telnet(CNC_host, CNC_port, 1)
        telnet_status = True
        tn_err_msg = False
        client.publish(f"ping/{topic}", "Telnet connected")

        client.publish(f"spBv1.0/FWH2200/DBIRTH/{asset}_RPI/{asset}", 'ONLINE', retain=True)

    except:
        telnet_status = False
        if not tn_err_msg:
            client.publish(f"error/{topic}", fail_message)
            print(fail_message)
            tn_err_msg = True
        else:
            client.publish(f"ping/{topic}", '')


with open("/home/pi/Haas-Data-Collection/DB Table columns.csv") as text:
    Q_codes = text.read()

Q_codes = Q_codes.split('\n')
Q_codes.pop(-1)
Description = []
for i,j in enumerate(Q_codes):
    Description.append(''.join(j.split(',')[1::]).replace(';',','))
    Q_codes[i] = j.split(',')[0]


# read data specific to setup and machines
with open("/home/pi/Haas-Data-Collection/Pub_config.txt") as config:
    # "/home/pi/Haas-Data-Collection/Pub_config.txt"
    mqttBroker = config.readline().split(" = ")[1].replace("\n", "")
    asset = config.readline().split(" = ")[1].replace("\n", "")
    CNC_host = config.readline().split(" = ")[1].replace("\n", "")

topic = asset
# topic = "HaasData"
CNC_port = 5051
MQTT_port = 1883
telnet_status = False
tn_err_msg = False

client = mqtt.Client(asset)
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.will_set(f"spBv1.0/FWH2200/NDEATH/{asset}_RPI", 'OFFLINE', retain=True)
client.connect(mqttBroker, MQTT_port, keepalive=10)
client.loop_start()

while not telnet_status:
    telnet_connection("Initial Telnet connection failed!")

last_out = []
omit = [Q_codes.index('?Q600 3012'),Q_codes.index('?Q300'), Q_codes.index('?Q600 3020')]

while True:

    if telnet_status:
        # transform Q-codes from table to binary and send to the CNC machine
        try:
            for i in Q_codes:
                msg = i.encode("ascii") + b"\n"
                tn.write(msg)
        except:
            client.publish(f"error/{topic}", "Telnet connection failed!: problem writing")
            telnet_status = False
            tn_err_msg = True

        try:
            out = tn.read_until(msg, 1).decode("utf-8").replace(">", '').replace("\r\n", "|").split("|")
            out.pop(-1)
            out[48] = "MACRO, "+ str(round(float(out[48].split(", ")[1]))) # round RPM to integer
        except:
            client.publish(f"error/{topic}", "Telnet connection failed!: problem reading")
            telnet_status = False
            tn_err_msg = True

        try:
            if last_out:
                new_out = out[:omit[0]] + out[omit[0] + 1:omit[1]] + out[omit[1] + 1:omit[2]] + out[omit[2] + 1:]
                if new_out == last_out:
                    print("pass")
                    client.publish(f"ping/{topic}", "")
                    time.sleep(1)
                    pass
                elif new_out:
                    print("new data")
                    publish_device_data(out)
                    last_out = new_out
                    time.sleep(1)
            else:
                print('initial message')
                if out:
                    publish_device_data(out)
                    last_out = out[:omit[0]] + out[omit[0] + 1:omit[1]] + out[omit[1] + 1:omit[2]] + out[omit[2] + 1:]
                time.sleep(1)

        except:
            client.publish(f"error/{topic}", "Parse and publish failed!")
            telnet_status = False
            tn_err_msg = True
    else:
        telnet_connection("Telnet connection failed!")