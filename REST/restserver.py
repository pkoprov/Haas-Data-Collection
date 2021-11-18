from flask import Flask, jsonify
from flask_restful import Api,Resource
import psycopg2 as pg


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
        uriObj["Machine coordinates"] = ".../XYZ"

        return uriObj

class Machines(Resource):
    def get(self):
        try:
            qCmd = 'SELECT * FROM public."CNC"'
            cur.execute(qCmd)
            records = cur.fetchall()

            return jsonify({'machineList': [rec for rec in records]})

        except (Exception, pg.DatabaseError) as error:
            print(error)


api.add_resource(Welcome, '/')
api.add_resource(All,'/all')
api.add_resource(Machines, '/machinelist')

if __name__ == '__main__':
    app.run(port='3000', debug=True)