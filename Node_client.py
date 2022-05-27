import os
import sys
import telnetlib
import time
import threading

import paho.mqtt.client as mqtt

sys.path.insert(0, r"C:\Users\pkoprov\PycharmProjects\Haas-Data-Collection\spb")
# sys.path.insert(0, "/home/pi/Haas-Data-Collection/spb")

import sparkplug_b as sparkplug
from sparkplug_b import *
from Device_client import NGC

def tn_connect(CNC_host, timeout):
    global tn_status, tn
    while True:
        if "tn_status" not in globals().keys() or not tn_status:
            try:
                tn = telnetlib.Telnet(CNC_host, port, timeout)
                tn_status = True
                print("Connected to CNC")

            except:
                tn_status = False
                time.sleep(1)
                if "tn_status_informed" not in locals().keys():
                    print("Connection to CNC failed")
                    tn_status_informed = True
                continue
            payload = sparkplug.getDdataPayload()
            addMetric(payload, "Device Status", None, MetricDataType.String, "start")
            totalByteArray = payload.SerializeToString()
            client.publish("spBv1.0/" + myGroupId + "/DCMD/" + myNodeName + "/" + myDeviceName, totalByteArray,
                           qos, False)


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
                        globals()['vf2'] = NGC(r'C:\Users\pkoprov\PycharmProjects\Haas-Data-Collection\DB Table columns.csv', tn, client)
                    elif metric.string_value == "stop":
                        print("Stopping of the Device program")
                        tn.close()
                        global tn_status
                        tn_status = False
                        time.sleep(3)

            # elif tokens[2] in ("DBIRTH", "DDEATH") and tokens[4] == myDeviceName:
            #     global dBirthTime, dDeathTime, tn_status
            #     dBirthTime = metric.timestamp
            #     if all([var in globals().keys() for var in ["dBirthTime", "dDeathTime"]]):
            #         if dBirthTime <= dDeathTime:
            #             tn_status = False
            #         else:
            #             tn_status = True
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
    global CNC_host, tn_status
    addMetric(payload, "CNC IP", None, MetricDataType.String, CNC_host)
    try:
        addMetric(payload, "CNC status", None, MetricDataType.Boolean, tn_status)
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
        addMetric(payload, "CNC status", None, MetricDataType.Boolean, tn_status)
    except:
        addMetric(payload, "CNC status", None, MetricDataType.Boolean, False)
    return payload


# read data specific to setup and machines
with open(r"C:\Users\pkoprov\PycharmProjects\Haas-Data-Collection\Node.config") as config:
    # "/home/pi/Desktop/Haas-Data-Collection/Pub_config.txt"
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

device_NGC = threading.Thread(target=tn_connect, args=(CNC_host, 1))
device_NGC.start()
time.sleep(3)



while True:
    publishNdata()
    if "vf2" in globals().keys() and vf2.tn_online:
        try:
            vf2.publishDeviceData()
        except NameError:
            pass

