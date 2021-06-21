# Haas Data Collection
## The project on collection of the data from Haas CNC machines
This project is implemented through installing the Rapberry PI as an end computing device that sends the request codes to the CNC machine and receives the data from it. Data transfer is implemented through telnet protocol by Ethernet port and thus is impossoble to hi-jack. RPI performs data wrangling and sends the message with JSON formatted data via Wi-Fi to the MQTT broker, which can be another RPI or any other computing device that is capable of running mosquitto server. The data from the MQTT broker is collected by a subsriber and is written to the PostgreSQL database.

## The code description
### Publisher.py
This is a code that runs on the RPI connected to the CNC machine and sends the data to the MQTT Broker. In order to run this code on boot the daemon file needs to be created. The file **publisher.service** needs to be copied to the folder /lib/systemd/system/. This can be performed by running the following in the command line:

`sudo cp /lib/systemd/system/publisher.service`

The **publisher.service** is included in github repo.
### Pub_config.txt
This file is used in the **Publisher.py**. It contains IP address of the CNC machine, IP address of the broker and the name of the machine. This file needs to be editted every time for each machine.
### DB Table columns.xlsx
This table contains the commands and macros that are to be send to CNC machine in order to retrieve the data. Sheet "Static" contains the static data for the CNC machines and is used to create the asset table in the database. Sheets with variables for VF-2 and ST-10 contain the commands and macros for th data that varies depending on either time or the motion of machines. These sheets are used to create tables for every CNC machine that will be used to collect the data from.
### Q-codes.py
Run this code to generate all the commands and macros that you have in the file **"Book of Macros.xlsx"**.
### Q-codes.xlsx
The table that is created after running the **Q-codes.py**.
### Create Table app.py
This code creates the tables in PostgreSQL database. You have to type the name of the tables (up to you how to call them) and type **Static** to create the asset table in the database. Furthermore, you have to type the name of the tables (up to you how to call them) and type names of the variables sheets from the **DB Table columns.xlsx** to create the tables for the every CNC machine you are gathering the data from.
### Subscriber.py
