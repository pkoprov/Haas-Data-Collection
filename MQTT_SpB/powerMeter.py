import serial


class PowerMeter:
    def __init__(self, port, baudrate = 115200, timeout=1):
        self.port = port
        self.baudrate = baudrate
        self.timeout = timeout
        self.ser = serial.Serial(port, baudrate)
        print("Serial port is connected")

    def Irms(self):
        self.ser.flushInput()
        data = self.ser.readline().decode()
        try:
            data = [float(i) for i in data.replace("\r\n", "").split(", ")]
        except ValueError:
            return None
        return tuple(data)
