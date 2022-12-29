# Haas Data Collection

## The project on collection of the data from Haas CNC machines

This project is implemented through installing the Raspberry PI (RPI) as an end computing device that sends the request
codes to the CNC machine and receives the data from it. Data transfer is implemented through telnet protocol by Ethernet
port and thus is impossible to hi-jack. RPI performs data wrangling and sends the message, formatted in compliance with
SparkplugB(c) specification, to the MQTT broker, which can be another RPI or any other computing device that can run
MQTT broker. The data from the MQTT broker is collected by a Historian and is written to the PostgreSQL database.

## Folders structure:

.  
├── **ammeter**&emsp;&emsp;&emsp;&emsp; &emsp; &emsp; &emsp; &emsp; # folder with the Arduino code for the ammeter  
├── **Dashboard**&emsp;&emsp;&emsp; &emsp; &emsp; &emsp; &emsp; # contains the files for the Grafana dashboard. ==Under
development==  
│ ├── [dash_publisher.py](Dashboard/dash_publisher.py)      
│ └── [dash_subscriber.py](Dashboard/dash_subscriber.py)  
├── **libs** &emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp; &emsp;&emsp;&emsp;&nbsp; # contains the libraries for the
MQTT SparkplugB(c) connection  
├── **MQTT_SpB**&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp; # contains scripts for MQTT clients  
│ ├── [Device client.py](MQTT_SpB/Device_client.py) &emsp;&emsp; &emsp;&emsp;&emsp; # script to talk to CNC machine and
device client  
│ ├── [Historian.py](MQTT_SpB/Historian.py)&emsp;&emsp;&emsp; &emsp;&emsp;&emsp;&emsp; # script to record data to DB  
│ ├── [Node_client.py](MQTT_SpB/Node_client.py)&emsp; &emsp; &emsp; &emsp;&emsp; # Edge of Node client  
│ └── [powerMeter.py](MQTT_SpB/powerMeter.py)&emsp; &emsp; &emsp; &emsp;&emsp; # helper function to get Irms from
Arduino  
├── **OPCUA**&emsp; &emsp; &emsp; &emsp; &emsp; &emsp; &emsp;&emsp; # folder with OPCUA server. ==Under development==  
├── **REST**&emsp; &emsp;&emsp; &emsp; &emsp; &emsp; &emsp; &emsp;&emsp; # REST server for dash. ==Under development==  
├── **spb**&emsp;&emsp;&emsp; &emsp; &emsp; &emsp;&emsp;&emsp; &emsp; &emsp; # SparkplugB(c) library  
├── **[Create Table app.py](Create%20Table%20app.py)** &emsp; &emsp; &emsp; # helper function to create tables in DB  
├── **[DB Table columns.csv](DB%20Table%20columns.csv)** &emsp; &emsp; # file with macros to be used ind Device client  
├── **[DB Table columns.xlsx](DB%20Table%20columns.xlsx)** &emsp;&emsp; # spreadsheet with macros  
├── *historian.config*&emsp; &emsp; &emsp;&emsp;&emsp; &emsp;# config file to be used for Historian.py   
├── *historian.service*&emsp; &emsp; &emsp;&emsp;&emsp; &emsp;# daemon to run on boot in Linux systems   
├── *Node.config* &emsp; &emsp; &emsp; &emsp; &emsp; &emsp; # config file to be used for Node_client.py  
├── *node_client.service*&emsp;&emsp; &emsp; &emsp; # daemon to run on RPI boot  
├── *Q-codes.py*&emsp;&emsp;&emsp; &emsp; &emsp; &emsp;&emsp; # helper function to create csv file with Q-codes  
├── *Q-codes.xlsx*&emsp;&emsp;&emsp;&emsp;&emsp;&emsp; &emsp; # spreadsheet that is created after running the Q-codes.py  
├── *README.md*&emsp;&emsp;&emsp;&emsp;&emsp;&emsp; &emsp;# this file  
└── *requirements.txt*&emsp;&emsp; &emsp; &emsp; &emsp; # list of required libraries

## The code description

### [Node_client.py](MQTT_SpB/Node_client.py)

This is a code that runs on the RPI connected to the CNC machine and sends the data to the MQTT Broker. To run this code
on boot the daemon file needs to be created. The file **node_client.service** must be copied to the folder
/lib/systemd/system/. This can be performed by running the following in the command line:

```
sudo cp node_client.service /lib/systemd/system/node_client.service
```

After the file was copied to the root folder it needs to be enabled by the following commands:

```
sudo systemctl daemon-reload
sudo systemctl enable node_client.service
```

The daemon will start working right after reboot. If you want to start the daemon immediately run following line:

```
sudo systemctl start node_client.service
```

To check the status of the daemon run the following line:

```
sudo systemctl status node_client.service
```


### [Node.config](Node.config)

This file is used in the **Node_client.py**. It contains IP address of the CNC machine, IP address of the broker and the
name of the machine. This file needs to be edited every time for each machine.

### [DB Table columns.xlsx](DB%20Table%20columns.xlsx")

This table contains the commands and macros that are to be send to CNC machine to retrieve the data. Sheet "Static"
contains the static data for the CNC machines and is used to create the asset table in the database. Sheets with
variables for VF-2 and ST-10 contain the commands and macros for the data that varies depending on either time or the
motion of machines. These sheets are used to create tables for every CNC machine that will be used to collect the data
from.

### [Q-codes.py](Q-codes.py)

Run this code to generate all the commands and macros that you have in the file **"Book of Macros.xlsx"**.

### [Q-codes.xlsx](Q-codes.xlsx)

The table that is created after running the **Q-codes.py**.

### [Create Table app.py](Create%20Table%20app.py)

This code creates the tables in PostgreSQL database. You must type the name of the tables (up to you how to call them)
and type **Static** to create the asset table in the database. Furthermore, you have to type the name of the tables (up
to you how to call them) and type names of the variable’s sheets from the **DB Table columns.xlsx** to create the tables
for every CNC machine you are gathering the data from.

### [Historian.py](MQTT_SpB/Historian.py)

This code runs the subscriber app that collects the data from the MQTT broker and makes row insertions to the PostgreSQL
database. This app will throw an error if the tables with corresponding names are not created. Every serial number of
the machine must correspond to the table name. Thus, the asset table with the static data should be filled manually with
amended column that contains the names of the corresponding tables.
This app runs on the same machine where the database is running. The IP address of the broker and the database
information should be kept in the _historian.config_ file.

### [historian.config](historian.config)

This file is the input parameters for the **Historian.py** script.

### [historian.service](historian.service)

This file is the daemon service to be run on Linux systems to record data to DB. 

The file **historian.service** must be copied to the folder /lib/systemd/system/. This can be performed by running the following in the command line:

```
sudo cp historian.service /lib/systemd/system/historian.service
```

After the file was copied to the root folder it needs to be enabled by the following commands:

```
sudo systemctl daemon-reload
sudo systemctl enable historian.service
```

The daemon will start working right after reboot. If you want to start the daemon immediately run following line:

```
sudo systemctl start historian.service
```

To check the status of the daemon run the following line:

```
sudo systemctl status historian.service
```

## The recommended software

Every RPI that is connected to the CNC machine must have the latest Python version and the libraries:

* Telnetlib
* Paho-mqtt
* Pandas
* Openpyxl
* SparkplugB (can be found in the [GitHub repo](https://github.com/eclipse/tahu))

Subscriber computing device must have the latest Python version and the libraries:

* Psycopg2
* Paho-mqtt
* Pandas
* Openpyxl.

Subscriber computing device must also have [PostgreSQL DB](https://www.postgresql.org/download/) and preferably
have [PGAdmin](https://www.pgadmin.org/download/) installed.
