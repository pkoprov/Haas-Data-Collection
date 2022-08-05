import sys
import paho.mqtt.client as mqtt

sys.path.insert(0, r"C:\Users\pkoprov\PycharmProjects\Haas-Data-Collection\spb") # uncomment for Windows
# sys.path.insert(0, "/home/pi/Haas-Data-Collection/spb") # uncomment for Raspberry Pi

from sparkplug_b import *


with open("Node.config") as config:
    mqttBroker = config.readline().split(" = ")[1].replace("\n", "")
    db = config.readline().split(" = ")[1].replace("\n", "")
    user = config.readline().split(" = ")[1].replace("\n", "")
    password = config.readline().split(" = ")[1].replace("\n", "")
    machines = config.readline().split(" = ")[1].replace("\n", "").split(', ')


######################################################################
# The callback for when the client receives a CONNACK response from the server.
######################################################################
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        client.subscribe("spBv1.0/FWH2200/DDATA/RPI_VF-2_1/#", 2)
        print("Connected with result code " + str(rc))
    else:
        print("Failed to connect with result code " + str(rc))
        sys.exit()


######################################################################
# The callback for when a PUBLISH message is received from the server.
######################################################################
def on_message(client, userdata, msg):
    global previous_Ddata, inboundPayload
    # print("Message arrived: " + msg.topic)
    inboundPayload = sparkplug_b_pb2.Payload()  # create a payload object
    inboundPayload.ParseFromString(msg.payload)  # parse the payload into the payload object
    if "previous_Ddata" in globals().keys():
        for metric in inboundPayload.metrics:
            if metric.name in ['Year, month, day', 'Hour, minute, second', 'Power-on Time (total)',
                               'Power on timer (read only)']:  # ignore these metrics
                continue
            for met in previous_Ddata.metrics:
                if met.name == metric.name:
                    previous_metric = met
                    break

            if previous_metric.datatype == MetricDataType.String and metric.string_value != previous_metric.string_value:
                print("{} changed from {} to {}".format(metric.name, previous_metric.string_value, metric.string_value))
                break
            elif previous_metric.datatype == MetricDataType.Float and metric.float_value != previous_metric.float_value:
                print("{} changed from {} to {}".format(metric.name, previous_metric.float_value, metric.float_value))
                break
            elif previous_metric.datatype == MetricDataType.Int32 and metric.int_value != previous_metric.int_value:
                print("{} changed from {} to {}".format(metric.name, previous_metric.int_value, metric.int_value))
                break
            elif previous_metric.datatype == MetricDataType.Boolean and metric.boolean_value != previous_metric.boolean_value:
                print("{} changed from {} to {}".format(metric.name, previous_metric.boolean_value, metric.boolean_value))
                break

    previous_Ddata = inboundPayload



client = mqtt.Client("Historian", clean_session=False)
client.on_connect = on_connect
client.on_message = on_message
client.connect(mqttBroker, 1883, 60)
while True:
    client.loop()
client.loop_start()
client.loop_stop()
client.loop()