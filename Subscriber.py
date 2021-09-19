import json
import psycopg2 as pg
import paho.mqtt.client as mqtt
import pandas as pd
from datetime import datetime, timedelta


with open("Sub_config.txt") as config:
    mqttBroker = config.readline().split(" = ")[1].replace("\n", "")
    db = config.readline().split(" = ")[1].replace("\n", "")
    user = config.readline().split(" = ")[1].replace("\n", "")
    password = config.readline().split(" = ")[1].replace("\n", "")
    machines = config.readline().split(" = ")[1].replace("\n", "").split(', ')

conn = pg.connect(f"dbname={db} user={user} password={password}")

try:
    cur = conn.cursor()
    print("Subscriber connection established")
except (Exception, pg.DatabaseError) as error:
    print(error)


def on_connect(client, userdata, flags, rc):
    client.publish("clients/redcollector21/subscriber", 'online', retain=True)
    print("Connected with Result Code: {}".format(rc))


def on_disconnect(client, userdata, flags, rc):
    client.publish("clients/redcollector21/subscriber", 'offline', retain=True)
    print("Disconnected with Result Code: {}".format(rc))


def on_message(client, userdata, message):
    msg = message.payload.decode("utf-8")

    if not msg:
        pass
    else:
        print(f"Received message in topic {message.topic}")
        if any([mach in message.topic for mach in machines]):
            machine = machines[[mach in message.topic for mach in machines].index(True)]
            if "data" in message.topic:
                try:
                    dataObj=json.loads(msg)
                    append_table(machine, dataObj)
                except:
                    print('Incorrect format of message!!! "' , msg, '"')

            elif 'clients' in message.topic:
                print(f"{machine} is {msg}")
                machine_state(machine, msg)

            elif 'error' in message.topic:
                print(f"{machine} has a problem: {msg}")
        else:
            print(f'{message.topic} is not in machines')


def insert_CMD(table, columns, values):
    CMD = f'INSERT INTO public."{table}" ({columns}) VALUES({values})'
    cur.execute(CMD)
    conn.commit()


def machine_state(machine, msg):
    CMD = f'SELECT * FROM public."{machine}" order by "Year, month, day" desc, "Hour, minute, second" desc limit 1'
    try:

        df = pd.read_sql_query(CMD, conn)

        if msg == "offline":
            if df['Three-in-one (PROGRAM, Oxxxxx, STATUS, PARTS, xxxxx)'][0] == "POWER OFF":
                pass
            else:
                if None in df.values:
                    columns = str(["Year, month, day", "Hour, minute, second",
                                   "Three-in-one (PROGRAM, Oxxxxx, STATUS, PARTS, xxxxx)"])[1:-1].replace("'", '"')
                    values = str([datetime.now().strftime("%y%m%d") + ".0", datetime.now().strftime("%H%M%S") + ".0",
                                  "POWER OFF"])[1:-1]
                    insert_CMD(machine, columns, values)
                else:
                    columns = str(list(df.columns))[1:-1].replace("'", '"')
                    df['Three-in-one (PROGRAM, Oxxxxx, STATUS, PARTS, xxxxx)'] = "POWER OFF"
                    df["Hour, minute, second"] = (datetime.now()+timedelta(seconds=15)).strftime("%H%M%S")+'.0'
                    values = str(df.values.tolist()[0])[1:-1]
                    insert_CMD(machine, columns, values)
        else:
            if df['Three-in-one (PROGRAM, Oxxxxx, STATUS, PARTS, xxxxx)'][0] == "POWER ON":
                pass
            else:
                columns = str(["Year, month, day", "Hour, minute, second",
                               "Three-in-one (PROGRAM, Oxxxxx, STATUS, PARTS, xxxxx)"])[1:-1].replace("'", '"')
                values = str([datetime.now().strftime("%y%m%d")+".0", datetime.now().strftime("%H%M%S")+".0",
                              "POWER ON"])[1:-1]
                insert_CMD(machine, columns, values)

    except (Exception, pg.DatabaseError) as error:
        print('machine_state error', error)
        conn.commit()


def append_table(table, message):
    try:

        # read column names for the current table
        cur.execute(f'SELECT * FROM public."{table}"')
        conn.commit()

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
        insert_CMD(table,columns,values)

    except (Exception, pg.DatabaseError) as error:
        print("row_insert error",error)


#MAIN
port=1883
client = mqtt.Client("redcollector21", True)
#call-back functions
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_message = on_message

client.will_set("clients/redcollector21/subscriber", 'offline', retain=True)
client.connect(mqttBroker, port)
client.subscribe("#")
client.loop_forever()
# client.loop_start()