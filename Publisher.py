import os
import sys
import telnetlib
import time

import paho.mqtt.client as mqtt

# sys.path.insert(0, "/home/pi/Haas-Data-Collection/spb")
sys.path.insert(0, r"C:\Users\pkoprov\PycharmProjects\Haas-Data-Collection\spb")

import sparkplug_b as sparkplug
from sparkplug_b import *


def tn_connect(CNC_host, timeout):
    global tn_status, tn
    while not tn_status:
        try:
            tn = telnetlib.Telnet(CNC_host, 5051, timeout)
            publishDeviceBirth()
            tn_status = True
        except:
            continue


######################################################################
# The callback for when the client receives a CONNACK response from the server.
######################################################################
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected with result code " + str(rc))
    else:
        print("Failed to connect with result code " + str(rc))
        sys.exit()

    global myGroupId
    global myNodeName

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("spBv1.0/" + myGroupId + "/NCMD/" + myNodeName + "/#")
    client.subscribe("spBv1.0/" + myGroupId + "/DCMD/" + myNodeName + "/#")


######################################################################
# The callback for when a PUBLISH message is received from the server.
######################################################################
def on_message(client, userdata, msg):
    print("Message arrived: " + msg.topic)
    tokens = msg.topic.split("/")

    if tokens[0] == "spBv1.0" and tokens[1] == myGroupId and tokens[3] == myNodeName:
        inboundPayload = sparkplug_b_pb2.Payload()
        inboundPayload.ParseFromString(msg.payload)
        for metric in inboundPayload.metrics:
            if tokens[2] == "NCMD":
                if metric.name == "Node Control/Next Server":
                    print("'Node Control/Next Server' is not implemented in this example")
                elif metric.name == "Node Control/Rebirth":
                    publishNodeBirth()
                    print("Node has been reborn")
                elif metric.name == "Node Control/Reboot":
                    print("Node is going to reboot")
                    try:
                        os.system('sudo reboot')
                    except:
                        print("Node reboot failed")
                elif metric.name == "bdSeq":
                    pass
                else:
                    print("Unknown command: " + metric.name)
            elif tokens[2] == "DCMD" and tokens[4] == myDeviceName:
                if metric.name == "Device Control/Rebirth":
                    publishDeviceBirth()
                    print("Device has been reborn")
                elif metric.name == "Device Control/Reboot":
                    global tn, tn_status
                    tn.close()
                    try:
                        tn.open(CNC_host, 5051, 1)
                        publishDeviceBirth()
                        print("Device has been rebooted")
                        tn_status = True
                    except:
                        print("Device reboot failed")
                elif metric.name == "bdSeq":
                    pass
                else:
                    print("Unknown command: " + metric.name)
    else:
        print("Unknown command...")

    print("Done publishing")


def disconnect(client, rc):
    deathPayload = sparkplug.getNodeDeathPayload()
    deathByteArray = deathPayload.SerializeToString()
    client.publish("spBv1.0/" + myGroupId + "/NDEATH/" + myNodeName, deathByteArray, 0, False)
    client.publish("spBv1.0/" + myGroupId + "/DDEATH/" + myNodeName + '/' + myDeviceName, deathByteArray, 0, False)
    print("Disconnected with Result Code: {}".format(rc))
    client.disconnect()


######################################################################
# Publish the NBIRTH certificate
######################################################################
def publishNodeBirth():
    print("Publishing Node Birth Certificate")

    # Create the node birth payload
    payload = sparkplug.getNodeBirthPayload()

    # Set up the Node Controls
    addMetric(payload, "Node Control/Next Server", None, MetricDataType.Boolean, False)
    addMetric(payload, "Node Control/Rebirth", None, MetricDataType.Boolean, False)
    addMetric(payload, "Node Control/Reboot", None, MetricDataType.Boolean, False)

    # Add some regular node metrics
    addMetric(payload, "Node time", None, MetricDataType.Float, time.time())
    global CNC_host, tn_status
    addMetric(payload, "CNC IP", None, MetricDataType.String, CNC_host)
    addMetric(payload, "CNC status", None, MetricDataType.Boolean, tn_status)

    globals()['previous_Ndata'] = payload
    # Publish the node birth certificate
    totalByteArray = payload.SerializeToString()
    client.publish("spBv1.0/" + myGroupId + "/NBIRTH/" + myNodeName, totalByteArray, 0, False)

    print("Node Birth Certificate has been published")


######################################################################
# Publish the DBIRTH certificate
######################################################################
def publishDeviceBirth():
    print("Publishing Device Birth Certificate")
    payload = getDdata()
    addMetric(payload, "Device Control/Rebirth", None, MetricDataType.Boolean, False)
    addMetric(payload, "Device Control/Reboot", None, MetricDataType.Boolean, False)

    globals()['previous_Ddata'] = payload
    totalByteArray = payload.SerializeToString()
    # Publish the initial data with the Device BIRTH certificate
    client.publish("spBv1.0/" + myGroupId + "/DBIRTH/" + myNodeName + "/" + myDeviceName, totalByteArray, 0, False)
    print("Device Birth Certificate has been published")


######################################################################
# Publish NDATA
######################################################################
def publishNdata():
    global previous_Ndata
    payload = getNdata()
    for i, metric in enumerate(payload.metrics):
        if metric.name == "Node time":
            continue
        elif metric.name == "CNC IP" and metric.string_value != \
                [met.string_value for met in previous_Ndata.metrics if met.name == metric.name][
                    0]:  # look for matching metric
            stale = False
            break
        elif metric.name == "CNC status" and metric.boolean_value != \
                [met.boolean_value for met in previous_Ndata.metrics if met.name == metric.name][
                    0]:  # look for matching metric
            stale = False
            break
        else:
            stale = True

    if 'stale' in locals().keys() and not stale:
        previous_Ndata = payload
        totalByteArray = payload.SerializeToString()
        client.publish("spBv1.0/" + myGroupId + "/NDATA/" + myNodeName, totalByteArray, 0, False)

        print("Node Data has been published")


######################################################################
# Get NDATA
######################################################################
def getNdata():
    payload = sparkplug_b_pb2.Payload()
    addMetric(payload, "Node time", None, MetricDataType.Float, time.time())
    addMetric(payload, "CNC IP", None, MetricDataType.String, CNC_host)
    addMetric(payload, "CNC status", None, MetricDataType.Boolean, tn_status)
    return payload


######################################################################
# retrieve data from NGC
######################################################################
def getDdata():
    global tn, par_list
    payload = sparkplug.getDdataPayload()

    # Add device metrics
    for par in par_list:
        code = par[-1].encode() + b"\n"
        tn.read_very_eager()
        try:
            tn.write(code)
        except:
            print(
                f"Telnet connection failed! Could not write {code} ({par[0]}) to CNC machine")  # if connection fails, try to reconnect

        msg = tn.read_until(b'\n', 1)
        value = parse(msg, par[0])
        if par[1] == 3:  # int
            value = int(float(value))
        elif par[1] == 9:  # float
            value = float(value)
        elif par[1] == 11:
            value = bool(float(value))
        addMetric(payload, par[0], None, par[1], value)

    return payload


# function to parse output of CNC machine from telnet port
def parse(telnetdata, par_name):
    msg = telnetdata.decode()[:-2].replace('>', '').split(', ')

    if len(msg) == 2 and msg[1] != "?":  # if value exists
        if 'year' in par_name.lower():
            val = time.strftime("%Y/%m/%d", time.strptime(msg[1][:-2], "%y%m%d"))
        elif 'hour' in par_name.lower():
            val = time.strftime("%H:%M:%S", time.strptime(msg[1][:-2], "%H%M%S"))
        elif 'rpm' in par_name.lower():
            val = int(float(msg[1]))
        else:
            val = msg[1]
    elif 'program' in ''.join(msg).lower() and "parts" in ''.join(msg).lower():
        val = str([msg[i] for i in (1, 2, 4)])[1:-1]
    elif 'busy' in ''.join(msg).lower():
        val = "busy"
    else:
        raise ValueError("No value found for parameter: " + par_name)
    return val


# read data specific to setup and machines
with open(r"C:\Users\pkoprov\PycharmProjects\Haas-Data-Collection\Pub_config.txt") as config:
    # "/home/pi/Desktop/Haas-Data-Collection/Pub_config.txt"
    mqttBroker = config.readline().split(" = ")[1].replace("\n", "")
    myGroupId = config.readline().split(" = ")[1].replace("\n", "")
    myNodeName = config.readline().split(" = ")[1].replace("\n", "")
    myDeviceName = config.readline().split(" = ")[1].replace("\n", "")
    CNC_host = config.readline().split(" = ")[1].replace("\n", "")
    myUsername = config.readline().split(" = ")[1].replace("\n", "")
    myPassword = config.readline().split(" = ")[1].replace("\n", "")

# read required parameters from csv file
with open(r"C:\Users\pkoprov\PycharmProjects\Haas-Data-Collection\DB Table columns.csv") as text:
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

# Create the node death payload
deathPayload = sparkplug.getNodeDeathPayload()
deathPayload.metrics[0].is_historical = True

# Start of main program - Set up the MQTT client connection
client = mqtt.Client(myNodeName, clean_session=True)
client.on_connect = on_connect
client.on_message = on_message
client.username_pw_set(myUsername, myPassword)
deathByteArray = deathPayload.SerializeToString()
client.will_set("spBv1.0/" + myGroupId + "/NDEATH/" + myNodeName, deathByteArray, 0, False)
client.connect(mqttBroker, 1883, 60)

# Short delay to allow connect callback to occur
client.loop_start()
time.sleep(1)

tn_status = False
# Publish Node birth certificate
publishNodeBirth()

tn_connect(CNC_host, 5)

while True:

    publishNdata()

    # check Telnet connection if persistent send Q-codes
    if tn_status:
        payload = getDdata()
    # if not, send DDEATH and reboot command to DCMD
    else:
        deathPayload = sparkplug.getNodeDeathPayload()
        deathByteArray = deathPayload.SerializeToString()
        client.publish("spBv1.0/" + myGroupId + "/DDEATH/" + myNodeName + '/' + myDeviceName, deathByteArray, 0, False)
        addMetric(deathPayload, "Device Control/Reboot", None, MetricDataType.Boolean, True)
        deathByteArray = deathPayload.SerializeToString()
        client.publish("spBv1.0/" + myGroupId + "/DCMD/" + myNodeName + '/' + myDeviceName, deathByteArray, 0, False)
        print("Device death published")
        time.sleep(1)
        # continue

    for i, metric in enumerate(payload.metrics):  # iterate through new metric's values to find if there was a change
        if metric.name in ['Year, month, day', 'Hour, minute, second', 'Power-on Time (total)',
                           'Power on timer (read only)']:  # ignore these metrics
            continue
        else:
            previous_metric = [met for met in previous_Ddata.metrics if met.name == metric.name][
                0]  # find previous metric matching current metric
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
                print(f"'{metric.name}' has changed")
                stale = False
                break

    if 'stale' in globals().keys() and not stale:
        totalByteArray = payload.SerializeToString()
        client.publish("spBv1.0/" + myGroupId + "/DDATA/" + myNodeName + '/' + myDeviceName, totalByteArray, 0, False)
        previous_Ddata = payload
        print("Device data has been published")
