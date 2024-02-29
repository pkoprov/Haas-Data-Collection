import sys
import os
import logging
import csv
import threading

sys.path.insert(0, "/home/pi/Haas-Data-Collection/spb")  # uncomment for Raspberry Pi

import sparkplug_b as sparkplug
from sparkplug_b import *
import time
import telnetlib
from Node_client import read_config, setup_mqtt_client


class Device:
    def __init__(
        self, config_path="./Node.config", parameter_csv="./DB Table columns.csv"
    ):
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )
        self.config = read_config(config_path)
        # Use the function to read parameters from the CSV file
        self.par_list = self.parse_csv_parameters(parameter_csv)

        self.MAC_LIST = b"?E1064 -1\n?E1065 -1\n?E1066 -1\n"

        self.client = setup_mqtt_client(self.config)

        self.client.on_message = self.on_message
        self.client.on_connect = self.on_connect
        self.birth_published = threading.Event()

    def connect(self, broker=None, port=1883, keepalive=60):
        if not broker:
            broker = self.config["broker"]
        self.client.connect(broker, port, keepalive)
        self.client.loop_start()

    def disconnect(self):
        self.client.loop_stop()
        self.client.disconnect()
        logging.info("Device disconnected")

    def on_connect(self, client, ud, flags, rc):
        if rc == 0:
            logging.info("Device connected with result code %s", rc)
            client.subscribe(
                f"{ud['spBv']}/{ud['myGroupId']}/DCMD/{ud['myNodeName']}/#", ud["QoS"]
            )
            client.subscribe(
                f"{ud['spBv']}/{ud['myGroupId']}/DDEATH/{ud['myNodeName']}/{ud['myDeviceName']}",
                ud["QoS"],
            )
            time.sleep(0.2)
            self.publishDeviceBirth()
            self.birth_published.set()
        else:
            logging.error("Device Failed to connect with result code %s", rc)

    def on_message(self, client, ud, msg):
        logging.info("Device message arrived: %s", msg.topic)
        tokens = msg.topic.split("/")

        # Check if the message is intended for this device
        if (
            tokens[0] == ud["spBv"]
            and tokens[1] == ud["myGroupId"]
            and tokens[2] in ["DCMD", "DDEATH"]
        ):
            self.handle_message(tokens, msg, ud)
        else:
            logging.warning("Unknown command received: %s", msg.topic)

    def handle_message(self, tokens, msg, ud):
        if tokens[2] == "DCMD" and tokens[3] == ud["myNodeName"]:
            self.handle_command_message(msg, ud)
        elif tokens[2] == "DDEATH" and tokens[4] == ud["myDeviceName"]:
            self.handle_death_message(ud)
        else:
            logging.warning("Unhandled message type: %s", "/".join(tokens))

    def handle_command_message(self, msg, ud):
        inboundPayload = sparkplug_b_pb2.Payload()

        try:
            # Attempt to parse the inbound message payload
            inboundPayload.ParseFromString(msg.payload)
        except Exception as e:
            # Log the exception and return early if parsing fails
            logging.error("Failed to parse inbound payload: %s", e)
            return

        for metric in inboundPayload.metrics:
            if metric.name == "Device Control/Rebirth" and metric.boolean_value:
                ud["publishDeviceBirth"]()
                logging.info("Device has been reborn")
            elif metric.name == "Device Control/Reconnect":
                logging.info("Node is going to reboot")
                try:
                    ud["tn"].close()
                    ud["tn"] = open(ud["CNC_host"], 5051, 3)
                except Exception as e:
                    logging.error("Device reconnect failed: %s", e)
                    ud["tn"] = False
            elif metric.name == "bdSeq":
                pass
            else:
                logging.warning("Unknown command: %s", metric.name)

    def handle_death_message(self, ud):
        logging.info("Device Death Certificate has been published. Oh well...")
        # self.disconnect()
        # if ud["newdeath"]:
        #     logging.info("Device Death Certificate has been published. Exiting...")
        #     sys.exit()

    # function to get data from NGC
    def getDdata(self):
        payload = sparkplug.getDdataPayload()

        # Convert command list to byte string
        code = b"".join([f"{par[-1]}\n".encode() for par in self.par_list]) + b"|*|\n"
        # Clear buffer and send command
        try:
            self.tn.read_very_eager()
            self.tn.write(code)
        except Exception as e:
            logging.error(
                f"Telnet connection failed! Could not write codes to CNC machine: {e}"
            )
            raise ConnectionError("Failed to write to CNC machine.")

        # Read response
        try:
            msg = self.tn.read_until(b"|*|", timeout=1)
            if not msg.endswith(b"|*|"):
                raise ConnectionError("Incomplete message received from CNC machine.")
        except Exception as e:
            logging.error(
                f"Telnet connection failed! Could not read from CNC machine: {e}"
            )
            raise ConnectionError("Failed to read from CNC machine.")

        # Parse the message
        try:
            val_list = self.parse(msg)
        except ValueError as e:
            logging.error(f"Error parsing message: {e}")
            raise

        # Process and add metrics
        for n, par in enumerate(self.par_list):
            value = val_list[n]
            try:
                if par[1] == 3:  # int
                    value = int(float(value))
                elif par[1] == 9:  # float
                    value = float(value)
                elif par[1] == 11:  # boolean
                    value = bool(float(value))
                elif par[1] == 12:  # string
                    value = str(value)
            except ValueError as e:
                logging.warning(f"Conversion error for {par[0]}: {e}")
                continue

            if "Max axis load" in par[0] and value == -1:
                continue
            addMetric(payload, par[0], None, par[1], value)

        # Send macros to NGC
        try:
            self.tn.write(self.MAC_LIST)
            self.tn.read_until(
                b">>!\r\n" * len(self.MAC_LIST.split(b"\n")[:-1]), timeout=1
            )
        except Exception as e:
            logging.error(
                f"Telnet connection failed! Could not write {self.MAC_LIST} to CNC machine: {e}"
            )
            raise ConnectionError("Failed to write macros to CNC machine.")

        return payload

    # function to parse output of CNC machine from telnet port
    def parse(self, telnet_data):
        val_list = []
        # Decode and split the telnet data into lines
        msg_list = telnet_data.decode().replace(">", "").split("\r\n")

        for msg, par in zip(msg_list, self.par_list):
            # Split the message by comma and space
            msg_parts = msg.split(", ")

            # Check if the message is valid and has an expected value
            if len(msg_parts) == 2 and msg_parts[1] != "?":
                key, raw_val = msg_parts
                try:
                    # Apply specific parsing rules based on the parameter type
                    if "year" in par[0].lower():
                        val = time.strftime(
                            "%Y/%m/%d", time.strptime(raw_val[:-2], "%y%m%d")
                        )
                    elif "hour" in par[0].lower():
                        val = time.strftime(
                            "%H:%M:%S", time.strptime(raw_val[:-2], "%H%M%S")
                        )
                    elif "rpm" in par[0].lower():
                        val = int(float(raw_val))
                    else:
                        val = raw_val
                except ValueError as e:
                    logging.error(f"Error parsing {par[0]} with value {raw_val}: {e}")
                    continue  # Skip this value on error
            elif "program" in msg.lower() and "parts" in msg.lower():
                val = ", ".join(msg_parts)
            elif "busy" in msg.lower():
                val = "BUSY"
            else:
                logging.warning(f"No value found for parameter: {par[0]}")
                continue  # Skip this parameter if no valid value is found

            val_list.append(val)

        return val_list

    # function to publish device birth certificate
    def publishDeviceBirth(self):
        logging.info("Publishing Device Birth Certificate")
        self.config["stale_notice"] = True

        try:
            payload = self.getDdata()
        except Exception as e:
            logging.error(f"Could not get data from CNC machine: {e}")
            self.tn.close()
            self.tn = False
            self.disconnect()
            return

        totalByteArray = payload.SerializeToString()
        topic = f"{self.config['spBv']}/{self.config['myGroupId']}/DBIRTH/{self.config['myNodeName']}/{self.config['myDeviceName']}"
        # Publish the device birth certificate with QoS as per config and retain flag True
        self.client.publish(
            topic, totalByteArray, qos=int(self.config["QoS"]), retain=True
        )
        time.sleep(0.1)  # Small delay to ensure message is published before proceeding
        logging.info("Device Birth Certificate has been published")
        self.config["stale"] = True
        # Example of storing data for later comparison or usage, if needed
        self.config["previous_Ddata"] = payload
        # return payload

    # function to publish device data
    def publishDeviceData(self):
        self.birth_published.wait()

        (
            logging.info("Attempting to publish device data...")
            if self.config["stale_notice"]
            else 0
        )

        try:
            payload = self.getDdata()
        except Exception as e:
            logging.error("Could not get data from CNC machine: %s", e)
            self.tn.close()
            self.tn = False
            self.publishDeviceDeath()
            self.disconnect()
            return

        for metric in payload.metrics:
            if metric.name in [
                "Year, month, day",
                "Hour, minute, second",
                "Power-on Time (total)",
                "Power on timer (read only)",
            ]:
                continue
            previous_metric = next(
                (
                    m
                    for m in self.config["previous_Ddata"].metrics
                    if m.name == metric.name
                ),
                None,
            )
            if previous_metric is None:
                logging.info(f"\nDevice metric has been added:\n{metric}")
                self.config["stale"] = False
                break
            if not self.has_metric_changed(previous_metric, metric):
                self.config["stale"] = True
                continue  # No significant change, continue checking
            else:
                logging.info(f"\nDevice metric has changed:\n{metric}")
                self.config["stale"] = False
                break

        if self.config["stale"]:
            (
                logging.info("No changes in device data. Skipping publication.")
                if self.config["stale_notice"]
                else 0
            )
            self.config["stale_notice"] = False
            return

        totalByteArray = payload.SerializeToString()
        topic = f"{self.config['spBv']}/{self.config['myGroupId']}/DDATA/{self.config['myNodeName']}/{self.config['myDeviceName']}"
        self.client.publish(
            topic, totalByteArray, qos=int(self.config["QoS"]), retain=True
        )

        logging.info("Device data has been published.")
        self.config["previous_Ddata"] = payload
        self.config["stale_notice"] = True
        # return payload  # Return the current payload for external tracking

    def has_metric_changed(self, previous_metric, current_metric):

        # Special case for "Coolant level" with a threshold for change
        if current_metric.name == "Coolant level":
            if abs(current_metric.float_value - previous_metric.float_value) < 2:
                return False  # Change is within the threshold, not significant

        # Check for changes based on data type
        if previous_metric.datatype == current_metric.datatype:
            if previous_metric.datatype == MetricDataType.String:
                return current_metric.string_value != previous_metric.string_value
            elif previous_metric.datatype == MetricDataType.Float:
                return current_metric.float_value != previous_metric.float_value
            elif previous_metric.datatype == MetricDataType.Int32:
                return current_metric.int_value != previous_metric.int_value
            elif previous_metric.datatype == MetricDataType.Boolean:
                return current_metric.boolean_value != previous_metric.boolean_value
        else:
            # If datatype itself has changed, consider it a significant change
            return True

        # If none of the above conditions met, assume no significant change
        return False

    def publishDeviceDeath(self):
        self.birth_published.wait()
        logging.info("Publishing Device Death Certificate")

        deathPayload = sparkplug.getDdataPayload()

        # Add "Device Control/Reboot" metric to the deathPayload
        addMetric(
            deathPayload, "Device Control/Reboot", None, MetricDataType.Boolean, True
        )

        totalByteArray = deathPayload.SerializeToString()
        topic = f"{self.config['spBv']}/{self.config['myGroupId']}/DDEATH/{self.config['myNodeName']}/{self.config['myDeviceName']}"

        # Publish the device death certificate with QoS 2 and retain flag True
        self.client.publish(
            topic, totalByteArray, qos=int(self.config["QoS"]), retain=True
        )
        logging.info("Device Death Certificate has been published")

        self.config["newdeath"] = True
        time.sleep(10)

    def setup_telnet_connection(self):
        """Attempt to establish the Telnet connection based on provided configuration."""
        try:
            self.tn = telnetlib.Telnet(self.config["CNC_host"], 5051, 3)
            logging.info("Connected to CNC machine via Telnet.")
            return self.tn
        except Exception as e:
            logging.error("Cannot connect to CNC machine: %s", e)
            self.tn = False

    # Function to parse the CSV file and return a list of parameter tuples
    def parse_csv_parameters(self, file_path):
        par_list = []
        with open(file_path, "r", newline="") as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                code = row[0]
                name = "".join(row[1:-1]).replace(";", ",")
                data_type_str = row[-1]
                data_type = {
                    "boolean": MetricDataType.Boolean,
                    "str": MetricDataType.String,
                    "float": MetricDataType.Float,
                    "int": MetricDataType.Int32,
                }.get(
                    data_type_str, MetricDataType.String
                )  # Default to String if unknown
                par_list.append((name, data_type, code))
        return tuple(par_list)


if __name__ == "__main__":
    path = os.path.dirname(__file__)
    vf2 = Device(f"{path}/../Node.config", f"{path}/../DB Table columns.csv")
    vf2.setup_telnet_connection()
    vf2.connect()
    time.sleep(0.2)

    while vf2.tn:
        vf2.publishDeviceData()
        time.sleep(0.1)
