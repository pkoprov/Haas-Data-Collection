import os
import sys
import threading
import time

import paho.mqtt.client as mqtt

# sys.path.insert(0, r"C:\Users\pkoprov\PycharmProjects\Haas-Data-Collection\spb")
sys.path.insert(0, "/home/pi/Haas-Data-Collection/spb")

import sparkplug_b as sparkplug
from sparkplug_b import *


def device_ping(device_ip, timeout=1):
    global device_online
    while True:
        if os.system("ping -c 1 " + device_ip + ' | grep "1 received"') == 0:
#         if os.system("ping -c 1 " + device_ip + ' | find "Received = 1"') == 0:
            if "device_online" not in globals().keys() or not device_online:
                payload = sparkplug.getDdataPayload()
                addMetric(payload, "Device Status", None, MetricDataType.String, "start")
                totalByteArray = payload.SerializeToString()
                client.publish("spBv1.0/" + myGroupId + "/DCMD/" + myNodeName + "/" + myDeviceName, totalByteArray,
                               qos, False)
                time.sleep(3)
        else:
            print("CNC is dead")
        time.sleep(timeout)


######################################################################
# The callback for when the client receives a CONNACK response from the server.
######################################################################
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        client.subscribe("spBv1.0/" + myGroupId + "/NCMD/" + myNodeName + "/#")
        client.subscribe("spBv1.0/" + myGroupId + "/DCMD/" + myNodeName + "/#")
        client.subscribe("spBv1.0/" + myGroupId + "/DDEATH/" + myNodeName + "/#")
        client.subscribe("spBv1.0/" + myGroupId + "/DBIRTH/" + myNodeName + "/#")
        print("Connected with result code " + str(rc))
    else:
        print("Failed to connect with result code " + str(rc))
        sys.exit()


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
                if metric.name == "Device Status":
                    if metric.string_value == "start":
                        print("Starting of the Device program")
                        os.system("python ./Device_client.py")

            elif tokens[2] in ("DBIRTH", "DDEATH") and tokens[4] == myDeviceName:

                if time.time() - inboundPayload.timestamp / 1000 > 1:
                    pass
                else:
                    global device_online
                    if tokens[2] == "DBIRTH":
                        print("Device Birth Certificate has been received")
                        device_online = True

                    elif tokens[2] == "DDEATH":
                        print("Device Death Certificate has been received")
                        device_online = False  # set device offline
    else:
        print("Unknown command...")


def disconnect(client, rc):
    deathPayload = sparkplug.getNodeDeathPayload()
    deathByteArray = deathPayload.SerializeToString()
    client.publish("spBv1.0/" + myGroupId + "/NDEATH/" + myNodeName, deathByteArray, qos, ret)
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
    global CNC_host, device_online
    addMetric(payload, "CNC IP", None, MetricDataType.String, CNC_host)
    try:
        addMetric(payload, "CNC status", None, MetricDataType.Boolean, device_online)
    except:
        addMetric(payload, "CNC status", None, MetricDataType.Boolean, False)

    globals()['previous_Ndata'] = payload
    # Publish the node birth certificate
    totalByteArray = payload.SerializeToString()
    client.publish("spBv1.0/" + myGroupId + "/NBIRTH/" + myNodeName, totalByteArray, qos, ret)

    print("Node Birth Certificate has been published")


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
            print(f"Metric has changed:\n{metric}")
            break
        elif metric.name == "CNC status" and metric.boolean_value != \
                [met.boolean_value for met in previous_Ndata.metrics if met.name == metric.name][
                    0]:  # look for matching metric
            stale = False
            print(f"\nNode metric has changed:\n{metric}")
            break
        else:
            stale = True

    if 'stale' in locals().keys() and not stale:
        totalByteArray = payload.SerializeToString()
        client.publish("spBv1.0/" + myGroupId + "/NDATA/" + myNodeName, totalByteArray, qos, ret)
        previous_Ndata = payload
        print("^Node Data has been published\n")


######################################################################
# Get NDATA
######################################################################
def getNdata():
    payload = sparkplug_b_pb2.Payload()
    addMetric(payload, "Node time", None, MetricDataType.Float, time.time())
    addMetric(payload, "CNC IP", None, MetricDataType.String, CNC_host)
    try:
        addMetric(payload, "CNC status", None, MetricDataType.Boolean, device_online)
    except:
        addMetric(payload, "CNC status", None, MetricDataType.Boolean, False)
    return payload


# read data specific to setup and machines
# with open(r"C:\Users\pkoprov\PycharmProjects\Haas-Data-Collection\Node.config") as config:
with open("/home/pi/Haas-Data-Collection/Node.config") as config:
    mqttBroker = config.readline().split(" = ")[1].replace("\n", "")
    myGroupId = config.readline().split(" = ")[1].replace("\n", "")
    myNodeName = config.readline().split(" = ")[1].replace("\n", "")
    myDeviceName = config.readline().split(" = ")[1].replace("\n", "")
    CNC_host = config.readline().split(" = ")[1].replace("\n", "")
    myUsername = config.readline().split(" = ")[1].replace("\n", "")
    myPassword = config.readline().split(" = ")[1].replace("\n", "")

# Create the node death payload
deathPayload = sparkplug.getNodeDeathPayload()
deathPayload.metrics[0].is_historical = True

# Start of main program - Set up the MQTT client connection
qos = 2
ret = True
client = mqtt.Client(myNodeName, clean_session=True)
client.on_connect = on_connect
client.on_message = on_message
client.username_pw_set(myUsername, myPassword)
deathByteArray = deathPayload.SerializeToString()
client.will_set("spBv1.0/" + myGroupId + "/NDEATH/" + myNodeName, deathByteArray, qos, ret)
client.connect(mqttBroker, 1883, 60)

# Short delay to allow connect callback to occur
client.loop_start()
port = 5051
time.sleep(1)

# Publish Node birth certificate
publishNodeBirth()

device_NGC = threading.Thread(target=device_ping, args=(CNC_host, 1))
device_NGC.start()
time.sleep(3)

while True:
    publishNdata()
