import sys
import time
import paho.mqtt.client as mqtt
sys.path.insert(0, r"C:\Users\pkoprov\PycharmProjects\Haas-Data-Collection\spb")
# sys.path.insert(0, "/home/pi/Haas-Data-Collection/spb")

import sparkplug_b as sparkplug
from sparkplug_b import *



def Hello_world():

    print("Hello World")

if __name__ == "__main__":

    with open(r"C:\Users\pkoprov\PycharmProjects\Haas-Data-Collection\Node.config") as config:
        # "/home/pi/Desktop/Haas-Data-Collection/Pub_config.txt"
        mqttBroker = config.readline().split(" = ")[1].replace("\n", "")
        myGroupId = config.readline().split(" = ")[1].replace("\n", "")
        myNodeName = config.readline().split(" = ")[1].replace("\n", "")
        myDeviceName = config.readline().split(" = ")[1].replace("\n", "")
        CNC_host = config.readline().split(" = ")[1].replace("\n", "")
        myUsername = config.readline().split(" = ")[1].replace("\n", "")
        myPassword = config.readline().split(" = ")[1].replace("\n", "")
    client = mqtt.Client("Test", clean_session=True)
    client.connect(mqttBroker, 1883, 60)
    client.loop_start()

    payload = sparkplug.getDdataPayload()
    addMetric(payload, "Device Control/Reboot", None, MetricDataType.Boolean, False)
    totalByteArray = payload.SerializeToString()
    client.publish("spBv1.0/" + myGroupId + "/DBIRTH/" + myNodeName + "/" + myDeviceName, totalByteArray, 2, True)
    time.sleep(3)

    for i in range(3):
        print("Iteration: {}".format(i))
        time.sleep(2)
        Hello_world()

    payload = sparkplug.getDdataPayload()
    addMetric(payload, "Device Control/Reboot", None, MetricDataType.Boolean, False)
    totalByteArray = payload.SerializeToString()
    client.publish("spBv1.0/" + myGroupId + "/DDEATH/" + myNodeName + "/" + myDeviceName, totalByteArray, 2, True)
    time.sleep(3)

    sys.exit()