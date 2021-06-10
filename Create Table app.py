import psycopg2 as pg
import pandas as pd
import easygui


db = "HaasDataCollection"
conn = pg.connect(f"dbname={db} user=postgres password='fwh2200'")


try:
    cur = conn.cursor()
    print("Subscriber connection established")
except (Exception, pg.DatabaseError) as error:
    print(error)


topic = input("Type the topic:  ")
path = easygui.fileopenbox()
sheet = input("Type the sheet name: ")
while True:
    try:
        colnames = pd.read_excel(path, sheet_name=sheet)
        break
    except ValueError:
        print("Incorrect sheet name")
        sheet = input("Type the CORRECT sheet name: ")
# Q_codes = Q_codes.append(pd.read_excel("Q-codes.xlsx"), ignore_index = True)
print(colnames)

columns = ""
for col in colnames["Description"]:
    columns = f'{columns}, "{col}" text'
columns = columns[2:]

def createTable():
    insertCMD = f'CREATE TABLE IF NOT EXISTS public."{topic}" ({columns});'
    cur.execute(insertCMD)

try:
    createTable()
    conn.commit()
    print(f"Table {topic} was created in DB {db}")
except(Exception, pg.DatabaseError) as error:
    print(error)