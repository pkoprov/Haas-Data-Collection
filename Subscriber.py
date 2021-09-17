import json
import psycopg2 as pg
import paho.mqtt.client as mqtt


with open("Sub_config.txt") as config:
    mqttBroker = config.readline().split(" = ")[1].replace("\n", "")
    db = config.readline().split(" = ")[1].replace("\n", "")
    user = config.readline().split(" = ")[1].replace("\n", "")
    password = config.readline().split(" = ")[1].replace("\n", "")

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


def subscribe(state):
    if state == True:
        print("Subscription started")
        client.subscribe("VF-2_1")
        client.subscribe("VF-2_2")
        client.subscribe("ST-10_1")
        client.subscribe("ST-10_2")
    else:
        client.unsubscribe("VF-2_1")
        client.unsubscribe("VF-2_2")
        client.unsubscribe("ST-10_1")
        client.unsubscribe("ST-10_2")
        print("Subscription stopped")

def on_message(client, userdata, message):
    msg = message.payload.decode("utf-8")
    if not msg:
        pass
    else:
        print("Received message in topic", message.topic)

        if msg == "Disconnected":
            print("Unexpected power off:", msg)
        else:
            try:
                dataObj=json.loads(msg)
                row_insert(message.topic, dataObj)
            except:
                print('Incorrect format of message!!! "' , msg, '"')


def row_insert(topic, message):
    try:

        # read column names for the current table
        cur.execute(f'SELECT * FROM public."{topic}"')
        conn.commit()
        # print(table_name)

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
        CMD = f'INSERT INTO public."{topic}" ({columns}) VALUES({values})'
        cur.execute(CMD)
        conn.commit()

    except (Exception, pg.DatabaseError) as error:
        print(error)


#MAIN
port=1883
client = mqtt.Client("Subscriber")
#call-back functions
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_message = on_message

client.will_set("clients/redcollector21/subscriber", 'offline', retain=True)
client.connect(mqttBroker, port)
subscribe(True)
client.loop_forever()
