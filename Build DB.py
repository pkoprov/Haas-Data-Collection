import csv
import psycopg2 as pg

conn = pg.connect(host="localhost", database = 'postgres',user='postgres',password='7323')


try:
    cur = conn.cursor()
    print("Subscriber connection established")
except (Exception, pg.DatabaseError) as error:
    print("Connection error {}".format(error))


with open("Haas Data Collection.csv", 'r') as file:
    out = csv.reader(file)
    for row in out:
        print(row)
        if ("SERIAL NUMBER") in row[0]:
            serial=int(row[1])
        elif ('SOFTWARE VERSION') in row[0]:
            soft_v = row[1].split(" ")[1]
        elif ('MODEL') in row[0]:
            model = row[1].split(" ")[1]

        insertQ = """ INSERT INTO Haas."CNC_Data" ("Model","Serial #", "Software Version") VALUES(%s,%s,%s)"""
        record = (model, serial, soft_v)

        try:
            cur.execute(insertQ, record)
            print("DB Transaction executed")

            # commit all transactions after the loop has stopped.
            conn.commit()

        except (Exception, pg.DatabaseError) as error:
            print("insertion error {}".format(error))