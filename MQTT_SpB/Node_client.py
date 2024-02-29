import os
import sys
import logging
import time
import platform
import subprocess
import paho.mqtt.client as mqtt

sys.path.insert(0, "/home/pi/Haas-Data-Collection/spb")  # uncomment for Raspberry Pi

import sparkplug_b as sparkplug
from sparkplug_b import *

# Improved error handling and logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


######################################################################
# function to ping the NGC
######################################################################
def device_ping(config, timeout=1):
    import platform
    import subprocess

    ping_cmd = (
        "ping -c 1 " + config["CNC IP"]
        if platform.system() != "Windows"
        else "ping -n 1 " + config["CNC IP"]
    )
    success_indicator = (
        "1 received" if platform.system() != "Windows" else "Received = 1"
    )
    try:
        # Perform the ping check
        response = subprocess.run(
            ping_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
            text=True,
        )
        if success_indicator in response.stdout:
            if not config["device_online"] and start_device_program:
                logging.info("Starting the Device program")
                subprocess.run(start_device_program, shell=True)
            config["device_online"] = True
        else:
            logging.info("NGC is not reachable")
            config["device_online"] = False
    except Exception as e:
        logging.error(f"Failed to ping {config['CNC IP']}: {e}")
        config["device_online"] = False

    time.sleep(timeout)
    return config["device_online"]


######################################################################
# The callback for when the client receives a CONNACK response from the server.
######################################################################
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logging.info("Node connected with result code %s", rc)
        topics = ["NCMD", "DCMD", "DDEATH", "DBIRTH"]
        for topic in topics:
            client.subscribe(
                f"{userdata['spBv']}/{userdata['myGroupId']}/{topic}/{userdata['myNodeName']}/#",
                userdata["QoS"],
            )
    else:
        logging.error("Node Failed to connect with result code %s", rc)
        sys.exit()


######################################################################
# The callback for when a PUBLISH message is received from the server.
######################################################################
def on_message(client, userdata, msg):
    logging.info("Node message arrived: %s", msg.topic)
    tokens = msg.topic.split("/")

    # Check if the message is intended for this device
    if tokens[0] == userdata["spBv"] and tokens[1] == userdata["myGroupId"]:
        handle_node_message(tokens, msg, userdata)
    else:
        logging.warning("Unknown command received: %s", msg.topic)


def handle_node_message(tokens, msg, userdata):
    if tokens[3] == userdata["myNodeName"]:
        if tokens[2] == "NCMD":
            handle_node_command(msg, userdata)
        elif tokens[2] in ["DBIRTH", "DDEATH"]:
            handle_device_life_cycle(msg, tokens[2], userdata)
        elif tokens[2] == "DCMD" and tokens[4] == userdata["myDeviceName"]:
            handle_device_command(msg, userdata)
    else:
        logging.warning("Unhandled node message type: %s", "/".join(tokens))


def handle_node_command(msg, userdata):
    inboundPayload = sparkplug_b_pb2.Payload()
    try:
        inboundPayload.ParseFromString(msg.payload)
    except Exception as e:
        logging.error("Failed to parse inbound payload: %s", e)
        return

    for metric in inboundPayload.metrics:
        process_node_command_metric(metric, userdata)


def process_node_command_metric(metric, userdata):
    if metric.name == "Node Control/Rebirth":
        logging.info("Node rebirth command received.")
        # TODO
        userdata["publishNodeBirth"]()
    elif metric.name == "Node Control/Reboot":
        logging.info("Node reboot command received.")
        try:
            os.system("sudo reboot")  # Be cautious with system reboot commands
        except Exception as e:
            logging.error("Node reboot failed: %s", e)
    elif metric.name == "bdSeq":
        pass  # Placeholder for future command handling
    else:
        logging.warning("Unknown node command: %s", metric.name)


def handle_device_life_cycle(msg, messageType, userdata):
    logging.info("Device life cycle message received: %s", messageType)
    inboundPayload = sparkplug_b_pb2.Payload()

    try:
        inboundPayload.ParseFromString(msg.payload)
    except Exception as e:
        logging.error("Failed to parse inbound payload for device life cycle: %s", e)
        return

    # 'dBirthTime' and 'dDeathTime' are used to track the device's life cycle status
    if messageType == "DBIRTH":
        userdata["dBirthTime"] = inboundPayload.timestamp
        logging.info(
            "Device birth certificate received at timestamp: %s", userdata["dBirthTime"]
        )
    elif messageType == "DDEATH":
        userdata["dDeathTime"] = inboundPayload.timestamp
        logging.info(
            "Device death certificate received at timestamp: %s", userdata["dDeathTime"]
        )

    userdata["device_online"] = userdata["dBirthTime"] > userdata["dDeathTime"]


def handle_device_command(msg, userdata):
    inboundPayload = sparkplug_b_pb2.Payload()
    try:
        inboundPayload.ParseFromString(msg.payload)
    except Exception as e:
        logging.error("Failed to parse inbound payload for device command: %s", e)
        return

    for metric in inboundPayload.metrics:
        process_device_command_metric(metric, userdata)


def process_device_command_metric(metric, userdata):
    if metric.name == "Device Control/Start":
        logging.info("Start command received for the device.")
        # Add your logic here to start the device or perform related actions
    elif metric.name == "Device Control/Stop":
        logging.info("Stop command received for the device.")
        # Add your logic here to stop the device or perform related actions
    else:
        logging.warning("Unknown device command: %s", metric.name)


def disconnect(client, userdata, rc):
    if rc == 0:  # Assuming rc == 0 indicates a clean disconnect
        try:
            deathPayload = sparkplug.getNodeDeathPayload()
            deathByteArray = deathPayload.SerializeToString()
            topic = f"{userdata['spBv']}/{userdata['myGroupId']}/NDEATH/{userdata['myNodeName']}"
            client.publish(topic, deathByteArray, userdata["QoS"], 1)
            logging.info("Published node death message successfully.")
        except Exception as e:
            logging.error("Failed to publish node death message: %s", e)
    else:
        logging.warning("Disconnecting with non-clean result code: %s", rc)

    try:
        client.disconnect()
        logging.info("Disconnected successfully.")
    except Exception as e:
        logging.error("Error during disconnect: %s", e)


######################################################################
# Publish the NBIRTH certificate
######################################################################
def publishNodeBirth(client, config):
    logging.info("Publishing Node Birth Certificate")
    userdata = client.user_data_get()
    userdata["stale_notice"] = True

    try:
        # Create the node birth payload
        payload = getNdata(config)

        # Set up the Node Controls
        addMetric(
            payload, "Node Control/Next Server", None, MetricDataType.Boolean, False
        )
        addMetric(payload, "Node Control/Rebirth", None, MetricDataType.Boolean, False)
        addMetric(payload, "Node Control/Reboot", None, MetricDataType.Boolean, False)

        userdata["previous_Ndata"] = payload

        # Serialize the payload for publishing
        totalByteArray = payload.SerializeToString()
        topic = f"{userdata['spBv']}/{userdata['myGroupId']}/NBIRTH/{userdata['myNodeName']}"
        client.publish(topic, totalByteArray, userdata["QoS"], 1)

        logging.info("Node Birth Certificate has been published")
        userdata["stale"] = True

    except Exception as e:
        logging.error("Failed to publish Node Birth Certificate: %s", e)


######################################################################
# Publish NDATA
######################################################################
def publishNdata(client):

    userdata = client.user_data_get()

    (
        logging.info("Attempting to publish Node data...")
        if userdata["stale_notice"]
        else 0
    )

    payload = getNdata(config)

    # Improved metric comparison logic
    for metric in payload.metrics:
        if metric.name == "Node time":
            continue  # Skip comparison for "Node time"

        # Retrieve the corresponding previous metric, if it exists
        if userdata["previous_Ndata"]:
            previous_metric = next(
                (
                    met
                    for met in userdata["previous_Ndata"].metrics
                    if met.name == metric.name
                ),
                None,
            )

        # Compare and detect changes
        if previous_metric:
            if (
                metric.HasField("string_value")
                and metric.string_value != previous_metric.string_value
            ):
                userdata["stale"] = False
                logging.info(f"Metric has changed: {metric}")
                break
            elif (
                metric.HasField("boolean_value")
                and metric.boolean_value != previous_metric.boolean_value
            ):
                userdata["stale"] = False
                logging.info(f"\nNode metric has changed: {metric}")
                break

    if not userdata["stale"]:
        try:
            totalByteArray = payload.SerializeToString()
            topic = (
                f"{client['spBv']}/{client['myGroupId']}/NDATA/{client['myNodeName']}"
            )
            client.publish(topic, totalByteArray, client["QoS"], 1)
            logging.info("^Node Data has been published\n")
            userdata["previous_Ndata"] = payload
            userdata["stale_notice"] = True
            return payload
        except Exception as e:
            logging.error(f"Failed to publish Node data: {e}")
    else:
        userdata["stale"] = True
        (
            logging.info("Node data is stale. No publication necessary.")
            if userdata["stale_notice"]
            else 0
        )
        userdata["stale_notice"] = False

    return None


######################################################################
# Get NDATA
######################################################################
def getNdata(config):
    payload = sparkplug_b_pb2.Payload()
    # Current time as a float metric
    addMetric(payload, "Node time", None, MetricDataType.Float, time.time())

    # CNC IP address as a string metric
    addMetric(payload, "CNC IP", None, MetricDataType.String, config["CNC IP"])

    # CNC status (online/offline) as a boolean metric
    addMetric(
        payload, "CNC status", None, MetricDataType.Boolean, config["device_online"]
    )
    return payload


def read_config(config_path="./Node.config"):
    """Read configuration from the file and return it as a dictionary."""
    config = {}
    with open(config_path) as f:
        for line in f:
            key, value = line.strip().split(" = ")
            config[key] = value
    return config


def setup_mqtt_client(config, is_node=False):
    """Setup MQTT client with user data from config."""

    client_id = config["myNodeName"] if is_node else config["myDeviceName"]
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION1, client_id, clean_session=True
    )

    # Set username and password if provided in config
    if "myUsername" in config and "myPassword" in config:
        client.username_pw_set(config["myUsername"], config["myPassword"])

    # Set user data for use in callbacks
    client.user_data_set(
        {
            "spBv": config["spBv"],
            "myGroupId": config["myGroupId"],
            "myNodeName": config["myNodeName"],
            "myDeviceName": config["myDeviceName"],
            "CNC_host": config["CNC_host"],
            "newdeath": False,
            "powerMeter": config.get("powerMeter", False) == "True",
            "QoS": int(config["QoS"]),
        }
    )

    if is_node:
        deathPayload = sparkplug.getNodeDeathPayload()
        deathPayload.metrics[0].is_historical = True
        deathByteArray = deathPayload.SerializeToString()
        userdata = client.user_data_get()
        will_topic = f"{userdata['spBv']}/{userdata['myGroupId']}/NDEATH/{userdata['myNodeName']}"
        client.will_set(
            topic=will_topic, payload=deathByteArray, qos=userdata["QoS"], retain=True
        )
    return client


if __name__ == "__main__":

    start_device_program = "python3 ./MQTT_SpB/Device_client.py"
    config = read_config()
    config["device_online"] = False
    config["dBirthTime"] = 0
    config["dDeathTime"] = 0

    client = setup_mqtt_client(config)

    # Setup callbacks
    client.on_message = on_message
    client.on_connect = on_connect

    # # Start of main program - Set up the MQTT client connection
    client.connect(config["MQTT Broker IP"], 1883, 60)
    client.loop_start()
    time.sleep(0.1)

    # Publish Node birth certificate
    publishNodeBirth(client, config)

    # device_ping(config)

    while True:
        publishNdata(client)  # publish NDATA if it changes
