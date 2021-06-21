# Haas Data Collection
## The project on collection of the data from Haas CNC machines
This project is implemented through installing the Rapberry PI as an end computing device that sends the request codes to the CNC machine and receives the data from it. Data transfer is implemented through telnet protocol by Ethernet port and thus is impossoble to hi-jack. RPI performs data wrangling and sends the message with JSON formatted data via Wi-Fi to the MQTT broker, which can be another RPI or any other computing device that can run mosquitto server. The data from the MQTT broker is collected by a subscriber and is written to the PostgreSQL database.

## The code description
### [Publisher.py](Publisher.py)
This is a code that runs on the RPI connected to the CNC machine and sends the data to the MQTT Broker. To run this code on boot the daemon file needs to be created. The file **publisher.service** must be copied to the folder /lib/systemd/system/. This can be performed by running the following in the command line: 
```
sudo cp /lib/systemd/system/publisher.service
```
After the file was copied to the root folder it needs to be enabled by the following commands:
```
sudo systemctl daemon-reload
sudo systemctl enable publisher.service
```
The daemon will start working right after reboot. If you want to start the daemon immediately run following line:
```
sudo systemctl start publisher.service
```
To check the status of the daemon run the follwoing line:
```
sudo systemctl status publisher.service
```

The **publisher.service** is included in github repo.
### [Pub_config.txt](Pub_config.txt)
This file is used in the **Publisher.py**. It contains IP address of the CNC machine, IP address of the broker and the name of the machine. This file needs to be edited every time for each machine.
### [DB Table columns.xlsx](DB%20Table%20columns.xlsx")
This table contains the commands and macros that are to be send to CNC machine to retrieve the data. Sheet "Static" contains the static data for the CNC machines and is used to create the asset table in the database. Sheets with variables for VF-2 and ST-10 contain the commands and macros for the data that varies depending on either time or the motion of machines. These sheets are used to create tables for every CNC machine that will be used to collect the data from.
### [Q-codes.py](Q-codes.py)
Run this code to generate all the commands and macros that you have in the file **"Book of Macros.xlsx"**.
### [Q-codes.xlsx](Q-codes.xlsx)
The table that is created after running the **Q-codes.py**.
### [Create Table app.py](Create%20Table%20app.py)
This code creates the tables in PostgreSQL database. You must type the name of the tables (up to you how to call them) and type **Static** to create the asset table in the database. Furthermore, you have to type the name of the tables (up to you how to call them) and type names of the variable’s sheets from the **DB Table columns.xlsx** to create the tables for every CNC machine you are gathering the data from.
### [Subscriber.py](Subscriber.py)
This code runs the subscriber app that collects the data from the MQTT broker and makes row insertions to the PostgreSQL database. This app will throw an error if the tables with corresponding names are not created. Every serial number of the machine must correspond to the table name. Thus, the asset table with the static data should be filled manually with amended column that contains the names of the corresponding tables.
This app runs on the same machine where the database is running. The IP address of the broker and the database information should be kept in the Sub_config.txt file.
### [Sub_config.txt](Sub_config.txt)
This file is the input parameters for the **Subscriber.py** app.
## The recommended software
Every RPI that is connected to the CNC machine must have the latest Python version and the libraries:
* Telnetlib
* Paho-mqtt
* Pandas
* Openpyxl.

Subscriber computing device must have the latest Python version and the libraries:
* Psycopg2
* Paho-mqtt
* Pandas
* Openpyxl.

Subscriber computing device must also have [PostgreSQL DB](https://www.postgresql.org/download/) and preferably have [PGAdmin](https://www.pgadmin.org/download/) installed.
