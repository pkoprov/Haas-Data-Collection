import sys

sys.path.insert(0, r"C:\Users\pkoprov\PycharmProjects\Haas-Data-Collection\spb")
# sys.path.insert(0, "/home/pi/Haas-Data-Collection/spb")

import sparkplug_b as sparkplug
from sparkplug_b import *
import time

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


class NGC(object):
    def __init__(self, par_csv, Telnet, client):
        self.tn = Telnet
        self.client = client
        # read required parameters from csv file
        with open(par_csv) as text:
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
        self.par_list = par_list

        self.publishDeviceBirth()
        self.tn_online = True

    # function to get data from CNC machine
    def getDdata(self):

        payload = sparkplug.getDdataPayload()
        n = 1
        # Add device metrics
        for par in self.par_list:
            code = par[-1].encode() + b"\n"
            try:
                self.tn.read_very_eager()
                self.tn.write(code)
            except:
                # if connection fails, try to reconnect
                print(f"Telnet connection failed! Could not write {code} ({par[0]}) to CNC machine")
                self.publishDeviceDeath()
                raise ConnectionError
            try:
                msg = self.tn.read_until(b'\n', 1)
            except:
                # if connection fails, try to reconnect
                print(f"Telnet connection failed! Could not read from CNC machine")
                self.publishDeviceDeath()
                raise ConnectionError
            try:
                value = self._parse(msg, par[0])
            except ValueError:
                print(f"{n} Empty value for {par[0]}")
                self.publishDeviceDeath()
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
    def _parse(self, telnetdata, par_name):
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
    def publishDeviceBirth(self):
        print("Publishing Device Birth Certificate")
        self.payload = self.getDdata()
        addMetric(self.payload, "Device Control/Rebirth", None, MetricDataType.Boolean, False)
        addMetric(self.payload, "Device Control/Reboot", None, MetricDataType.Boolean, False)

        totalByteArray = self.payload.SerializeToString()
        # Publish the initial data with the Device BIRTH certificate
        self.client.publish("spBv1.0/" + myGroupId + "/DBIRTH/" + myNodeName + "/" + myDeviceName, totalByteArray, 2,
                            True)

        print("Device Birth Certificate has been published")
        self.previous_Ddata = self.payload

    # function to publish device data
    def publishDeviceData(self):
        try:
            self.payload = self.getDdata()
        except:
            print("Could not get data from CNC machine")
            return

        for i, metric in enumerate(
                self.payload.metrics):  # iterate through new metrics values to find if there was a change
            if metric.name in ['Year, month, day', 'Hour, minute, second', 'Power-on Time (total)',
                               'Power on timer (read only)']:  # ignore these metrics
                continue
            else:
                previous_metric = [met for met in self.previous_Ddata.metrics if met.name == metric.name][
                    0]  # find previous metric matching current metric
                if metric.name == "Coolant level" and abs(
                        metric.float_value - previous_metric.float_value) <= 2:  # if coolant level is stable
                    self.stale = True
                    continue
                elif (
                        previous_metric.datatype == MetricDataType.String and metric.string_value == previous_metric.string_value) or (
                        previous_metric.datatype == MetricDataType.Float and metric.float_value == previous_metric.float_value) or (
                        previous_metric.datatype == MetricDataType.Int32 and metric.int_value == previous_metric.int_value) or (
                        previous_metric.datatype == MetricDataType.Boolean and metric.boolean_value == previous_metric.boolean_value):
                    self.stale = True
                    continue
                else:
                    print(f"\nDevice metric has changed:\n{metric}")
                    self.stale = False
                    break
        if self.stale:
            print(f"{self.payload.timestamp} Data is stale")

    def publishDeviceDeath(self):
        deathPayload = sparkplug.getNodeDeathPayload()
        print("Publishing Device Death Certificate")
        addMetric(deathPayload, "Device Control/Rebirth", None, MetricDataType.Boolean, False)
        addMetric(deathPayload, "Device Control/Reboot", None, MetricDataType.Boolean, True)
        totalByteArray = deathPayload.SerializeToString()
        self.client.publish("spBv1.0/" + myGroupId + "/DDEATH/" + myNodeName + "/" + myDeviceName, totalByteArray, 2,
                            True)
        print("Device Death Certificate has been published")
        self.tn_online = False


if __name__ == "__main__":
    vf2 = NGC(r'C:\Users\pkoprov\PycharmProjects\Haas-Data-Collection\DB Table columns.csv', tn)
    vf2.par_list
    vf2.tn.read_very_eager()
    payload = vf2.getDdata()
    vf2.publishDeviceData()
