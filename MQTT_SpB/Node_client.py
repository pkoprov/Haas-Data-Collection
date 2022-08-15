import os
import sys
import threading
import time

import paho.mqtt.client as mqtt

# sys.path.insert(0, r"C:\Users\pkoprov\PycharmProjects\Haas-Data-Collection\spb") # uncomment for Windows
sys.path.insert(0, "/home/pi/Haas-Data-Collection/spb") # uncomment for Raspberry Pi

import sparkplug_b as sparkplug
from sparkplug_b import *


######################################################################
# function to ping the NGC
######################################################################
def device_ping(device_ip, timeout=1):
    global device_online
    while True:
        if os.system("ping -c 1 " + device_ip + ' | grep "1 received"') == 0: # uncomment for Raspberry Pi
            #         if os.system("ping -c 1 " + device_ip + ' | find "Received = 1"') == 0: # uncomment for Windows

            if not device_online:
                print("Starting of the Device program")
                os.system("python3 ./Device_client.py")  # uncomment for Raspberry Pi
                # os.system("python Device_client.py") # uncomment for Windows
                time.sleep(3)
                device_online = True
        else:
            if device_online:
                print("NGC is not reachable")
                device_online = False
            else:
                pass
        time.sleep(timeout)


######################################################################
# The callback for when the client receives a CONNACK response from the server.
######################################################################
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        client.subscribe("spBv1.0/" + myGroupId + "/NCMD/" + myNodeName + "/#", qos)
        client.subscribe("spBv1.0/" + myGroupId + "/DCMD/" + myNodeName + "/#", qos)
        client.subscribe("spBv1.0/" + myGroupId + "/DDEATH/" + myNodeName + "/#", qos)
        client.subscribe("spBv1.0/" + myGroupId + "/DBIRTH/" + myNodeName + "/#", qos)
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

    # if the message is purposed for this node
    if tokens[0] == "spBv1.0" and tokens[1] == myGroupId and tokens[3] == myNodeName:
        inboundPayload = sparkplug_b_pb2.Payload() # create a payload object
        inboundPayload.ParseFromString(msg.payload) # parse the payload into the payload object
        if tokens[2] == "NCMD": # if the message is a node command
            for metric in inboundPayload.metrics:
                if metric.name == "Node Control/Next Server":
                    print("'Node Control/Next Server' is not implemented in this example")
                elif metric.name == "Node Control/Rebirth":
                    publishNodeBirth()
                    print("Node has been reborn")
                elif metric.name == "Node Control/Reboot":
                    print("Node is going to reboot")
                    try:
                        os.system('sudo reboot') # uncomment for Raspberry Pi
                    except:
                        print("Node reboot failed")
                elif metric.name == "bdSeq":
                    pass
                else:
                    print("Unknown command: " + metric.name)
        elif tokens[2] == "DCMD" and tokens[4] == myDeviceName: # if the message is a device command and for this device
            for metric in inboundPayload.metrics: # iterate through the metrics
                if metric.name == "Device Status":
                    if metric.string_value == "start":
                        print("Starting of the Device program")
                        os.system("python3 ./Device_client.py") # uncomment for Raspberry Pi

        elif tokens[2] in ("DBIRTH", "DDEATH") and tokens[4] == myDeviceName: # if the message is a device birth or death
            if time.time() - inboundPayload.timestamp / 1000 < 1: # if the message is within the last second
                global dBirthTime, dDeathTime, device_online
                if tokens[2] == "DBIRTH":
                    print("Device Birth Certificate has been received")
                    dBirthTime = inboundPayload.timestamp
                    device_online = True

                elif tokens[2] == "DDEATH":
                    print("Device Death Certificate has been received")
                    dDeathTime = inboundPayload.timestamp
                    device_online = False

                if all(var in globals().keys() for var in ['dBirthTime', 'dDeathTime']): # if both birth and death certificates have been received
                    if dDeathTime > dBirthTime: # if the death certificate is after the birth certificate
                        device_online = False
                    else:
                        device_online = True
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
    payload = getNdata()

    # Set up the Node Controls
    addMetric(payload, "Node Control/Next Server", None, MetricDataType.Boolean, False)
    addMetric(payload, "Node Control/Rebirth", None, MetricDataType.Boolean, False)
    addMetric(payload, "Node Control/Reboot", None, MetricDataType.Boolean, False)

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
    global device_online
    addMetric(payload, "CNC status", None, MetricDataType.Boolean, device_online)
    return payload


# read data specific to setup and machines
# with open(r"C:\Users\pkoprov\PycharmProjects\Haas-Data-Collection\Node.config") as config: # uncomment for Windows
with open("/home/pi/Haas-Data-Collection/Node.config") as config: # uncomment for Raspberry Pi
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
deathByteArray = deathPayload.SerializeToString()

# Start of main program - Set up the MQTT client connection
qos = 2
ret = True
client = mqtt.Client(myNodeName, clean_session=False)
client.on_connect = on_connect
client.on_message = on_message
client.username_pw_set(myUsername, myPassword)
client.will_set("spBv1.0/" + myGroupId + "/NDEATH/" + myNodeName, deathByteArray, qos, ret)
client.connect(mqttBroker, 1883, 60)

# Short delay to allow connect callback to occur
client.loop_start()
port = 5051 # port number for the Telnet server
time.sleep(1)

device_online = False # set the device online flag
# Publish Node birth certificate
publishNodeBirth()

device_NGC = threading.Thread(target=device_ping, args=(CNC_host, 1)) # thread for device pinging
device_NGC.start()
time.sleep(3)

while True:
    publishNdata() # publish NDATA if it changes
