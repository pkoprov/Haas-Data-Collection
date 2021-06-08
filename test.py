import telnetlib
import pandas as pd

CNC_host = "169.254.242.1"
CNC_port = 5051
tn = telnetlib.Telnet(CNC_host, CNC_port)
Q_codes = pd.read_excel("./Haas-Data-Collection/Q-codes.xlsx", sheet_name="Global")
Q_codes = Q_codes.append(pd.read_excel("./Haas-Data-Collection/Q-codes.xlsx",sheet_name="Macros"), ignore_index = True)

macros_dict = {}

for n, i in enumerate(Q_codes["Variable"]):
    msg = i.encode("ascii")+b"\n"
    tn.write(msg)

out = tn.read_until(msg,0.5).decode("utf-8").replace(">",'').replace("\r\n","|").split("|")
out.pop(-1)

for n, value in enumerate(out):
    val_list = value.split(", ")
    if value[1] != "?":
        macros_dict[Q_codes["Description"][n]] = val_list[1]