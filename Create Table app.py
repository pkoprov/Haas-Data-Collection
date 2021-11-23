import psycopg2 as pg
import pandas as pd


db = "HaasDataCollection"
conn = pg.connect(f"dbname={db} user=postgres password='fwh2200'")


def createTable():
    insertCMD = f'CREATE TABLE IF NOT EXISTS public."{topic}" ({columns});'
    cur.execute(insertCMD)

try:
    cur = conn.cursor()
    print("Subscriber connection established")
except (Exception, pg.DatabaseError) as error:
    print(error)

running = True
while running:
    topic = input("Type the table name:  ")
    path = "DB Table columns.xlsx"
    sheet = input("Type the sheet name: ")
    while True:
        try:
            colnames = pd.read_excel(path, sheet_name=sheet)
            break
        except ValueError:
            print("Incorrect sheet name")
            sheet = input("Type the CORRECT sheet name: ")
    # Q_codes = Q_codes.append(pd.read_excel("Q-codes.xlsx"), ignore_index = True)

    columns = ""
    for col in colnames["Description"]:
        columns = f'{columns}, "{col}" text'
    columns = columns[2:]

    try:
        createTable()
        conn.commit()
        print(colnames)
        print(f"Table {topic} was created in DB {db}")
    except(Exception, pg.DatabaseError) as error:
        print(error)

    while True:
        prompt = input("Create new table? ").lower()
        if  prompt == 'yes':
            running=True
            break
        elif prompt == 'no':
            running = False
            break
        else:
            print("Incorrect input")
