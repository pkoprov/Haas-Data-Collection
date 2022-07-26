import sys

import paho.mqtt.client as mqtt
import psycopg2 as pg

sys.path.insert(0, r'C:\Users\pkoprov\PycharmProjects\Haas-Data-Collection\spb')  # uncomment for Windows

from sparkplug_b import *

with open(r"C:\Users\pkoprov\PycharmProjects\Haas-Data-Collection\historian.config", 'r') as config: # uncomment for Windows
    mqttBroker = config.readline().split(" = ")[1].replace("\n", "")
    myGroupId = config.readline().split(" = ")[1].replace("\n", "")
    dbName = config.readline().split(" = ")[1].replace("\n", "")
    myUsername = config.readline().split(" = ")[1].replace("\n", "")
    myPassword = config.readline().split(" = ")[1].replace("\n", "")

conn = pg.connect(f"dbname={dbName} user={myUsername} password={myPassword}") # connect to DB

try:
    cur = conn.cursor() # create a cursor object
    print("Subscriber connection established")
except (Exception, pg.DatabaseError) as error:
    print(error)


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        client.subscribe("spBv1.0/" + myGroupId + "/#", qos) # subscribe to all topics in the group
        print("Connected with result code " + str(rc))
    else:
        print("Failed to connect with result code " + str(rc))
        sys.exit()


def on_message(client, userdata, msg):
    print("Message arrived: " + msg.topic)
    tokens = msg.topic.split("/")

    if tokens[0] == "spBv1.0" and tokens[1] == myGroupId: # check if the message is for this group
        global inboundPayload
        inboundPayload = sparkplug_b_pb2.Payload()  # create a payload object
        try:
            inboundPayload.ParseFromString(msg.payload)  # parse the payload into the payload object
        except Exception as e:
            print("Could not parse payload: ", e)
            return

        if tokens[2] == "NBIRTH":
            print('Action has not been implemented yet')
        elif tokens[2] == "DBIRTH":
            # insert data into DB
            append_table(tokens[4], inboundPayload, dBirth=True)
        elif tokens[2] == "NDEATH":
            try:
                # read what devices are connected to the EoN
                cur.execute('SELECT "Devices" FROM "AML"."EoN" WHERE "Nodename" = %s', (tokens[3],))  # get the last row
                conn.commit()
            except (Exception, pg.DatabaseError) as error:
                print("DB query error: ", error)
            devices = cur.fetchone()
            try:
                for dev in devices:
                    append_table(dev, None, dDeath=True)
                    print("NDEATH published to DB for device: ", dev)
            except:
                print("Failed to publish NDEATH to DB for device: ", tokens[3])
        elif tokens[2] == "DDEATH":
            append_table(tokens[4], None, dDeath=True)
            append_table(tokens[4], inboundPayload, dDeath=True)
        elif tokens[2] == "NDATA":
            print('Action has not been implemented yet')
        elif tokens[2] == "DDATA":
            # insert data into DB
            append_table(tokens[4], inboundPayload)
        elif tokens[2] == "NCMD":
            print('Action has not been implemented yet')
        elif tokens[2] == "DCMD":
            print('Action has not been implemented yet')
        elif tokens[2] == "STATUS":
            print('Action has not been implemented yet')
        else:
            print("Received unknown message from ", tokens[3:])
    else:
        print("Unknown message...")


def insert_CMD(table, columns, values):
    CMD = f'INSERT INTO "AML"."{table}" ({columns}) VALUES ({values})'
    cur.execute(CMD)
    conn.commit()


def append_table(table, message, dBirth=False, dDeath=False):
    try:
        # read column names for the current table
        cur.execute(f'SELECT * FROM "AML"."{table}" order by "Power on timer (read only)" desc limit 1') # get the last row
        conn.commit()
    except (Exception, pg.DatabaseError) as error:
        print("DB query error: ", error)

    try:
        dbData = cur.fetchone() # get the last row if it exists
    except:
        dbData = None # if there is no data, set dbData to None

    # create a string with column names
    col_list = [f'"{col[0]}"' for col in cur.description]
    columns = ", ".join(col_list)

    # create a string with values
    values = []

    if dDeath: # if the message is a death message
        for value, col in zip(dbData, col_list):
            value = str(value)
            if col == "Three-in-one (PROGRAM, Oxxxxx, STATUS, PARTS, xxxxx)":
                values.append("'DDEATH, DATA IS STALE'")
            else:
                values.append(f"'{value}'")
    else:
        for metric in message.metrics:
            if metric.name in columns:
                if 'Three-in-one' in metric.name and dBirth:
                    values.append("'DBIRTH'")
                elif metric.datatype == MetricDataType.Float:
                    values.append(f"'{metric.float_value}'")
                elif metric.datatype == MetricDataType.String:
                    values.append(f"'{metric.string_value}'")
                elif metric.datatype == MetricDataType.Int32:
                    values.append(f"'{metric.int_value}'")
                elif metric.datatype == MetricDataType.Boolean:
                    values.append(f"'{metric.boolean_value}'")

        # check if the last DB row is the same as the current messages
        if len(values) != len(col_list):
            print("Error: DB and MQTT payloads have different number of columns")
            return
        elif dbData is None: # if there is no data in the DB
            pass
        elif all([str(dbData[i]) == values[i][1:-1] for i in range(len(values))]): # if the data is the same
            return

    # convert list to string
    values = ", ".join(values)

    try:
        # commit insert query
        insert_CMD(table, columns, values)
    except (Exception, pg.DatabaseError) as error:
        print("row_insert error: ", error)


# setup MQTT client, callbacks and connection
qos = 2
client = mqtt.Client("Historian", False)
client.on_connect = on_connect
client.on_message = on_message
client.connect(mqttBroker, 1883, 60)

while True:
    client.loop()
