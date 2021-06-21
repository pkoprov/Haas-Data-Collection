# Haas Data Collection
## The project on collection of the data from Haas CNC machines
This project is implemented through installing the Rapberry PI as an end computing device that sends the request codes to the CNC machine and receives the data from it. Data transfer is implemented through telnet protocol by Ethernet port and thus is impossoble to hi-jack. RPI performs data wrangling and sends the message with JSON formatted data via Wi-Fi to the MQTT broker, which can be another RPI or any other computing device that is capable of running mosquitto server. The data from the MQTT broker is collected by a subsriber and is written to the PostgreSQL database.

## Python code description
### Publisher.py
This is a code that runs on the RPI connected to the CNC machine and sends the data to the MQTT Broker.
