import sys

# sys.path.insert(0, r"C:\Users\pkoprov\PycharmProjects\Haas-Data-Collection\spb")  # uncomment for Windows
sys.path.insert(0, "/home/pi/Haas-Data-Collection/spb")  # uncomment for Raspberry Pi

import sparkplug_b as sparkplug
from sparkplug_b import *
import time
import telnetlib
import paho.mqtt.client as mqtt


######################################################################
# The callback for when the client receives a CONNACK response from the server.
######################################################################
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        client.subscribe("spBv1.0/" + myGroupId + "/DCMD/" + myNodeName + "/#", qos)
        print("Connected with result code " + str(rc))
    else:
        print("Failed to connect with result code " + str(rc) + "\n")
        sys.exit()


######################################################################
# The callback for when a PUBLISH message is received from the server.
######################################################################
def on_message(client, userdata, msg):
    print("Device message arrived: " + msg.topic)
    tokens = msg.topic.split("/")

    # if the message is purposed for this device and is a command
    if tokens[0] == "spBv1.0" and tokens[1] == myGroupId and tokens[2] == "DCMD" and tokens[3] == myNodeName:
        inboundPayload = sparkplug_b_pb2.Payload()  # create a new payload object
        inboundPayload.ParseFromString(msg.payload)  # parse the payload into the payload object
        for metric in inboundPayload.metrics:  # iterate through the metrics in the payload
            if metric.name == "Device Control/Rebirth" and metric.boolean_value:
                publishDeviceBirth()
                print("Device has been reborn")
            elif metric.name == "Device Control/Reconnect":
                print("Node is going to reboot")
                global tn
                tn = tn.close()
                try:
                    tn = open(CNC_host, 5051, 3)
                except:
                    print("Device reconnect failed")
                    sys.exit()
            elif metric.name == "bdSeq":
                pass
            else:
                print("Unknown command: " + metric.name)
    else:
        print("Unknown command...")


# function to get data from NGC
def getDdata():
    payload = sparkplug.getDdataPayload()

    code = "".join([par[-1] + "\n" for par in par_list]).encode() + b"|*|\n"
    try:
        tn.read_very_eager()  # clear buffer
        tn.write(code)  # send command
    except:
        # if connection fails, try to reconnect
        print(f"Telnet connection failed! Could not write codes to CNC machine")
        raise ConnectionError
    try:
        msg = tn.read_until(b'|*|', 1)  # read response
    except:
        # if connection fails, try to reconnect
        print(f"Telnet connection failed! Could not read from CNC machine")
        raise ConnectionError
    if b"|*|" in msg:
        msg = msg[:-10]
    else:
        print(f"Telnet connection failed! Could not read all message from CNC machine")
        raise ConnectionError
    try:
        val_list = parse(msg, par_list)  # parse response
    except ValueError:
        print(f"{n} Empty value for {par[0]}")
        raise ConnectionError

    # Add device metrics
    for n, par in enumerate(par_list):  # iterate through the parameters
        if par[1] == 3:  # int
            val_list[n] = int(float(val_list[n]))
        elif par[1] == 9:  # float
            val_list[n] = float(val_list[n])
        elif par[1] == 11:  # boolean
            val_list[n] = bool(float(val_list[n]))
        elif par[1] == 12:  # string
            val_list[n] = str(val_list[n])
        if "Max axis load" in par[0] and val_list[n] == -1:
            continue
        addMetric(payload, par[0], None, par[1], val_list[n])

    # send macros to NGC
    try:
        tn.write(mac_list)
    except:
        print(f"Telnet connection failed! Could not write {mac} to CNC machine")
        raise ConnectionError

    tn.read_until(b'>>!\r\n' * len(mac_list.split(b"\n")[:-1]), timeout=1)  # flush the buffer after macros
    return payload


# function to parse output of CNC machine from telnet port
def parse(telnetdata, par_list):
    val_list = []
    msg_lst = telnetdata.decode().replace(">", "").split("\r\n")

    for msg, par in zip(msg_lst, par_list):
        msg = msg.split(", ")

        if len(msg) == 2 and msg[1] != "?":  # if value exists
            if 'year' in par[0].lower():
                val = time.strftime("%Y/%m/%d", time.strptime(msg[1][:-2], "%y%m%d"))
            elif 'hour' in par[0].lower():
                val = time.strftime("%H:%M:%S", time.strptime(msg[1][:-2], "%H%M%S"))
            elif 'rpm' in par[0].lower():
                val = int(float(msg[1]))
            else:
                val = msg[1]
        elif ('program' and "parts") in ''.join(msg).lower():
            val = ', '.join(msg)
        elif 'busy' in ''.join(msg).lower():
            val = "BUSY"
        else:
            raise ValueError("No value found for parameter: " + par[0])
        val_list.append(val)
    return val_list


# function to publish device birth certificate
def publishDeviceBirth():
    global previous_Ddata
    print("Publishing Device Birth Certificate")
    try:
        payload = getDdata()  # get data from NGC
    except:
        print("Could not get data from CNC machine")
        tn.close()
        sys.exit()

    totalByteArray = payload.SerializeToString()
    # Publish the initial data with the Device BIRTH certificate
    client.publish("spBv1.0/" + myGroupId + "/DBIRTH/" + myNodeName + "/" + myDeviceName, totalByteArray, 2,
                   True)
    time.sleep(0.1)
    print("Device Birth Certificate has been published")
    previous_Ddata = payload


# function to publish device data
def publishDeviceData():
    global previous_Ddata
    try:
        payload = getDdata()
    except:
        print("Could not get data from CNC machine")
        tn.close()
        publishDeviceDeath()
        sys.exit()
    
    # iterate through new metrics values to find if there was a change
    for i, metric in enumerate(payload.metrics):  
        if metric.name in ['Year, month, day', 'Hour, minute, second', 'Power-on Time (total)',
                           'Power on timer (read only)']:  # ignore these metrics
            continue
        else:  # find previous metric matching current metric
            try:
                previous_metric = [prev_met for prev_met in previous_Ddata.metrics if prev_met.name == metric.name][0]
            except:
                print(f"\nDevice metric has been added:\n{metric}")
                stale = False
                break
            if metric.name == "Coolant level" and abs(
                    metric.float_value - previous_metric.float_value) < 2:  # if coolant level is stable
                stale = True
                continue
            elif (
                    previous_metric.datatype == MetricDataType.String and metric.string_value == previous_metric.string_value) or (
                    previous_metric.datatype == MetricDataType.Float and metric.float_value == previous_metric.float_value) or (
                    previous_metric.datatype == MetricDataType.Int32 and metric.int_value == previous_metric.int_value) or (
                    previous_metric.datatype == MetricDataType.Boolean and metric.boolean_value == previous_metric.boolean_value):
                stale = True
                continue
            else:
                print(f"\nDevice metric has changed:\n{metric}")
                stale = False
                break
    if stale:
        return

    else:
        totalByteArray = payload.SerializeToString()
        # Publish the initial data with the Device BIRTH certificate
        client.publish("spBv1.0/" + myGroupId + "/DDATA/" + myNodeName + "/" + myDeviceName, totalByteArray, 2, True)

        print("^Device Data has been published")
        previous_Ddata = payload  # update previous data


def publishDeviceDeath():
    deathPayload = sparkplug.getDdataPayload()
    print("Publishing Device Death Certificate")
    addMetric(deathPayload, "Device Control/Reboot", None, MetricDataType.Boolean, True)
    totalByteArray = deathPayload.SerializeToString()
    client.publish("spBv1.0/" + myGroupId + "/DDEATH/" + myNodeName + "/" + myDeviceName, totalByteArray, 2, True)
    print("Device Death Certificate has been published")
    time.sleep(0.1)
    tn.close()
    sys.exit()


# read data specific to setup and machines
# with open(r"C:\Users\pkoprov\PycharmProjects\Haas-Data-Collection\Node.config") as config: # uncomment for Windows
with open("/home/pi/Haas-Data-Collection/Node.config") as config:  # uncomment for Raspberry Pi
    mqttBroker = config.readline().split(" = ")[1].replace("\n", "")
    myGroupId = config.readline().split(" = ")[1].replace("\n", "")
    myNodeName = config.readline().split(" = ")[1].replace("\n", "")
    myDeviceName = config.readline().split(" = ")[1].replace("\n", "")
    CNC_host = config.readline().split(" = ")[1].replace("\n", "")
    myUsername = config.readline().split(" = ")[1].replace("\n", "")
    myPassword = config.readline().split(" = ")[1].replace("\n", "")

try:
    tn = telnetlib.Telnet(CNC_host, 5051, 3)
except:
    print("Cannot connect to CNC machine")
    sys.exit()

qos = 2
ret = True
client = mqtt.Client(myDeviceName, clean_session=True)
client.on_connect = on_connect
client.on_message = on_message
client.username_pw_set(myUsername, myPassword)

client.connect(mqttBroker, 1883, 60)
client.loop_start()
time.sleep(0.1)

# read required parameters from csv file
# with open(r"C:\Users\pkoprov\PycharmProjects\Haas-Data-Collection\DB Table columns.csv") as text:  # uncomment for Windows
with open("/home/pi/Haas-Data-Collection/DB Table columns.csv") as text:  # uncomment for Raspberry Pi
    parameters = text.read().split('\n')[:-1]

# create parameter tuples
par_list = []
for i, par in enumerate(parameters):
    par = par.split(',')
    code = par[0]
    name = ''.join(par[1:-1]).replace(';', ',')
    if par[-1] == 'boolean':
        data_type = MetricDataType.Boolean
    elif par[-1] == 'str':
        data_type = MetricDataType.String
    elif par[-1] == 'float':
        data_type = MetricDataType.Float
    elif par[-1] == 'int':
        data_type = MetricDataType.Int32
    par_list.append((name, data_type, code))
par_list = tuple(par_list)

mac_list = b"?E1064 -1\n?E1065 -1\n?E3196 5000.0000\n"

publishDeviceBirth()  # publish birth certificate

while True:
    publishDeviceData()  # publish data if data has changed
    time.sleep(0.1)
