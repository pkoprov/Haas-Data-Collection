from opcua import Server

server = Server()

url = "opc.tcp://192.168.10.2:4840"
name = "OPCUA_sim"
server.set_endpoint(url)

addspace = server.register_namespace(name)
addspace2 = server.register_namespace(name+"2")

node = server.get_objects_node()
node2 = server.get_objects_node()
Param = node.add_object(addspace2, "Parameters")

Param = node.add_object(addspace, "Parameters")
NGC = node.add_object(addspace, "Haas_NGC")
Temp = Param.add_variable(addspace, "Temp", 12)