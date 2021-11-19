from flask import Flask, jsonify, request
from flask_restful import Api,Resource
import psycopg2 as pg
import pandas as pd


with open("../Sub_config.txt") as config:
    mqttBroker = config.readline().split(" = ")[1].replace("\n", "")
    db = config.readline().split(" = ")[1].replace("\n", "")
    user = config.readline().split(" = ")[1].replace("\n", "")
    password = config.readline().split(" = ")[1].replace("\n", "")
    machines = config.readline().split(" = ")[1].replace("\n", "").split(', ')


try:
    conn = pg.connect(f"dbname={db} user={user} password={password}")
    cur = conn.cursor()
    print("Subscriber connection established")
except (Exception, pg.DatabaseError) as error:
    print(error)

app = Flask(__name__)
api = Api(app)


class Welcome(Resource):
    def get(self):
        return "Welcome to the REST server: Type '.../all' for more commands"

class All(Resource):
    def get(self):
        uriObj = {}

        uriObj["list of resources"] = ".../all"
        uriObj["Machine List"] = ".../machinelist"
        uriObj["Last 200 records"] = ".../last200"
        uriObj["Last record"] = ".../last"
        uriObj["Machine coordinates"] = ".../XYZ?codename=<name of table>"

        return uriObj

class Machines(Resource):
    def get(self):
        try:
            data={}
            qCmd = 'SELECT * FROM public."CNC"'
            cur.execute(qCmd)
            records = cur.fetchall()

            for i, rec in enumerate(records):
                data[i]={}
                for j, desc in enumerate(cur.description):
                    data[i][desc.name]=rec[j]

            return jsonify('machineList', data)

        except (Exception, pg.DatabaseError) as error:
            print(error)


class MachineDataLast(Resource):

    def get(self):
        codename = request.args.get("codename")
        try:
            qCmd = f'SELECT * FROM public."{codename}" ORDER BY "Year, month, day" DESC, "Power-on Time (total)" DESC LIMIT 10'
            df = pd.read_sql_query(qCmd, conn)

            # df['temperature'] = df['temperature'].apply(lambda x: x[0])
            # df['pressure'] = df['pressure'].apply(lambda x: x[0])
            # df['humidity'] = df['humidity'].apply(lambda x: x[0])

            return df.to_json()

        except (Exception, pg.DatabaseError) as error:
            print(error)
            return jsonify({'msg': "Something went wrong..wrong entry"})


class Coordinates(Resource):

    def get(self):
        codename = request.args.get("codename")
        coord = 'Present machine coordinate position '
        try:
            qCmd = f'SELECT "{coord+"X"}","{coord+"Y"}","{coord+"Z"}" FROM public."{codename}" ' \
              f'order by "Year, month, day" desc, "Power-on Time (total)" desc limit 1'
            cur.execute(qCmd)
            row = cur.fetchall()
            xyz={"X":None,"Y":None,"Z":None}
            for i,j in enumerate(xyz.keys()):
                xyz[j] = float(row[0][i])
            return xyz

        except (Exception, pg.DatabaseError) as error:
            return error, jsonify({'msg': "Something went wrong..wrong entry"})

api.add_resource(Welcome, '/')
api.add_resource(All,'/all')
api.add_resource(Machines, '/machinelist')
api.add_resource(MachineDataLast,'/last10')
api.add_resource(Coordinates,'/XYZ')

if __name__ == '__main__':
    app.run(host= '10.76.152.200',port='3000', debug=True)