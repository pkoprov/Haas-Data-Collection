import sys

# sys.path.insert(0, r"C:\Users\pkoprov\PycharmProjects\Haas-Data-Collection\spb")
sys.path.insert(0, "/home/pi/Haas-Data-Collection/spb")

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
        client.subscribe("spBv1.0/" + myGroupId + "/DCMD/" + myNodeName + "/#")
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

    if tokens[0] == "spBv1.0" and tokens[1] == myGroupId and tokens[2] == "DCMD" and tokens[3] == myNodeName:
        inboundPayload = sparkplug_b_pb2.Payload()
        inboundPayload.ParseFromString(msg.payload)
        for metric in inboundPayload.metrics:
            if metric.name == "Device Control/Rebirth":
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


# function to get data from CNC machine
def getDdata():
    payload = sparkplug.getDdataPayload()
    n = 1
    # Add device metrics
    for par in par_list:
        code = par[-1].encode() + b"\n"
        try:
            tn.read_very_eager()
            tn.write(code)
        except:
            # if connection fails, try to reconnect
            print(f"Telnet connection failed! Could not write {code} ({par[0]}) to CNC machine")
            publishDeviceDeath()
            raise ConnectionError
        try:
            msg = tn.read_until(b'\n', 1)
        except:
            # if connection fails, try to reconnect
            print(f"Telnet connection failed! Could not read from CNC machine")
            publishDeviceDeath()
            raise ConnectionError
        try:
            value = parse(msg, par[0])
        except ValueError:
            print(f"{n} Empty value for {par[0]}")
            publishDeviceDeath()
            raise ConnectionError
        if par[1] == 3:  # int
            value = int(float(value))
        elif par[1] == 9:  # float
            value = float(value)
        elif par[1] == 11:  # boolean
            value = bool(float(value))
        elif par[1] == 12:  # string
            value = str(value)
        addMetric(payload, par[0], None, par[1], value)
        n += 1

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


# function to publish device birth certificate
def publishDeviceBirth():
    global previous_Ddata
    print("Publishing Device Birth Certificate")
    payload = getDdata()
    addMetric(payload, "Device Control/Rebirth", None, MetricDataType.Boolean, False)
    addMetric(payload, "Device Control/Reboot", None, MetricDataType.Boolean, False)

    totalByteArray = payload.SerializeToString()
    # Publish the initial data with the Device BIRTH certificate
    client.publish("spBv1.0/" + myGroupId + "/DBIRTH/" + myNodeName + "/" + myDeviceName, totalByteArray, 2,
                   True)

    print("Device Birth Certificate has been published")
    previous_Ddata = payload


# function to publish device data
def publishDeviceData():
    global previous_Ddata
    try:
        payload = getDdata()
    except ConnectionError:
        print("Could not get data from CNC machine")
        sys.exit()
        return

    for i, metric in enumerate(
            payload.metrics):  # iterate through new metrics values to find if there was a change
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
                print(f"\nDevice metric has changed:\n{metric}")
                stale = False
                break
    if stale:
        print(f"{payload.timestamp} Data is stale")

    else:
        addMetric(payload, "Device Control/Rebirth", None, MetricDataType.Boolean, False)
        addMetric(payload, "Device Control/Reboot", None, MetricDataType.Boolean, False)

        totalByteArray = payload.SerializeToString()
        # Publish the initial data with the Device BIRTH certificate
        client.publish("spBv1.0/" + myGroupId + "/DDATA/" + myNodeName + "/" + myDeviceName, totalByteArray, 2,
                       True)

        print("^Device Data has been published")

        previous_Ddata = payload


def publishDeviceDeath():
    deathPayload = sparkplug.getNodeDeathPayload()
    print("Publishing Device Death Certificate")
    addMetric(deathPayload, "Device Control/Rebirth", None, MetricDataType.Boolean, False)
    addMetric(deathPayload, "Device Control/Reboot", None, MetricDataType.Boolean, True)
    totalByteArray = deathPayload.SerializeToString()
    client.publish("spBv1.0/" + myGroupId + "/DDEATH/" + myNodeName + "/" + myDeviceName, totalByteArray, 2,
                   True)
    print("Device Death Certificate has been published")
    sys.exit()


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


try:
    tn = telnetlib.Telnet(CNC_host, 5051, 3)
except TimeoutError:
    print("Cannot connect to CNC machine")

qos = 2
ret = True
client = mqtt.Client(myDeviceName, clean_session=True)
client.on_connect = on_connect
client.on_message = on_message
client.username_pw_set(myUsername, myPassword)
# Create the node death payload
deathPayload = sparkplug.getNodeDeathPayload()
deathPayload.metrics[0].is_historical = True

deathByteArray = deathPayload.SerializeToString()
client.will_set("spBv1.0/" + myGroupId + "/DDEATH/" + myNodeName + "/" + myDeviceName, deathByteArray, qos, ret)
client.connect(mqttBroker, 1883, 60)
client.loop_start()

# read required parameters from csv file
# with open(r"C:\Users\pkoprov\PycharmProjects\Haas-Data-Collection\DB Table columns.csv") as text:
with open("/home/pi/Haas-Data-Collection/DB Table columns.csv") as text:
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


publishDeviceBirth()

while True:
    publishDeviceData()
