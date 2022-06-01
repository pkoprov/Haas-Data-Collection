import psycopg2 as pg
import pandas as pd


with open(r"C:\Users\pkoprov\PycharmProjects\Haas-Data-Collection\historian.config", 'r') as config:
    mqttBroker = config.readline().split(" = ")[1].replace("\n", "")
    myGroupId = config.readline().split(" = ")[1].replace("\n", "")
    dbName = config.readline().split(" = ")[1].replace("\n", "")
    myUsername = config.readline().split(" = ")[1].replace("\n", "")
    myPassword = config.readline().split(" = ")[1].replace("\n", "")

conn = pg.connect(f"dbname={dbName} user={myUsername} password={myPassword}")

def createTable():
    insertCMD = f'CREATE TABLE IF NOT EXISTS "{schema}"."{topic}" ({columns});'
    cur.execute(insertCMD)

try:
    cur = conn.cursor()
    print("Subscriber connection established")
except (Exception, pg.DatabaseError) as error:
    print(error)

schemas = cur.execute("select schema_name from information_schema.schemata;")
schemas = cur.fetchall()

schema = input(f"Enter schema ({'/'.join([i[0] for i in schemas])}): ")
path = r"C:\Users\pkoprov\PycharmProjects\Haas-Data-Collection\DB Table columns.xlsx"
file = pd.ExcelFile(path)

running = True
while running:
    topic = input("Type the table name: ")
    sheet = input(f"Type the sheet name ({'/'.join(file.sheet_names)}): ")
    while True:
        try:
            colnames = file.parse(sheet_name=sheet)
            break
        except ValueError:
            print("Incorrect sheet name")
            sheet = input("Type the CORRECT sheet name: ")
    # Q_codes = Q_codes.append(pd.read_excel("Q-codes.xlsx"), ignore_index = True)

    columns = []
    for i,row in colnames.iterrows():
        dtype = "text" if row["datatype"] == "str" else "numeric"
        columns.append(f'"{row[1]}" {dtype}, ')
    if sheet == "Static":
        columns.append('"IP_address" text')
        columns = "".join(columns)
    else:
        columns = "".join(columns)[:-2]


    try:
        createTable()
        conn.commit()
        print(f"Table {topic} was created in DB {dbName}")
    except(Exception, pg.DatabaseError) as error:
        print(error)

    while True:
        prompt = input("Create new table?(y/n): ").lower()
        if  prompt == 'y':
            running=True
            break
        elif prompt == 'n':
            running = False
            break
        else:
            print("Incorrect input")
