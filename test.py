import json, time
import psycopg2 as pg
import paho.mqtt.client as mqtt

db = "HaasDataCollection"
conn = pg.connect(f"dbname={db} user=postgres password='fwh2200'")


try:
    cur = conn.cursor()
    print("Subscriber connection established")
except (Exception, pg.DatabaseError) as error:
    print(error)


def on_connect(client, userdata, flags, rc):
    print("Connected with Result Code: {}".format(rc))

# def on_log(client, userdata, level, buffer):
#     print("Log: ", buffer)

def on_disconnect(client, userdata, rc=0):
    client.loop_stop()

def on_message(client, userdata, message):
    print("Received message:", message.topic)
    msg = message.payload.decode("utf-8")
    dataObj=json.loads(msg)
    print(dataObj["Machine Serial Number"])
    # row_insert(dataObj)

def row_insert(message):
    try:
        # read the CNC_machines table
        cur.execute('SELECT * FROM public."CNC_machines"')
        static_data = cur.fetchall()
        conn.commit()

        # define into which table to insert
        for row in static_data:
            if message["Machine Serial Number"] == row[2]:
                table_name = row[0]

        # read column names for the current table
        cur.execute(f'SELECT * FROM public."{table_name}"')
        conn.commit()
        print(table_name)

        # create a string with column names
        columns = ""
        for col in [col[0] for col in cur.description]:
            columns = f'{columns}, "{col}"'
        columns = columns[2:]

        # create a string with VALUES
        values = ''
        for col in [col[0] for col in cur.description]:
            try:
                values = f"{values}, '{message[col]}'"
            except KeyError:
                values = f"{values}, 'NaN'"
        values = values[2:]

        # commit insert query
        CMD = f'INSERT INTO public."{table_name}" ({columns}) VALUES({values})'
        cur.execute(CMD)
        conn.commit()

    except (Exception, pg.DatabaseError) as error:
        print(error)


#MAIN
mqttBroker = "192.168.10.2"
port=1883
client = mqtt.Client("Subscriber")
client.connect(mqttBroker)

#call-back functions
client.on_connect = on_connect
# client.on_log = on_log
client.on_message = on_message


client.loop_start()

client.subscribe("VF-2_1")
client.subscribe("VF-2_2")
client.subscribe("ST-10_1")
client.subscribe("ST-10_2")
time.sleep(30)

client.loop_stop()

