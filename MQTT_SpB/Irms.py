def Irms(ser):
    ser.flushInput()
    data = ser.readline().decode()
    try:
        data = [float(i) for i in data.replace("\r\n","").split(", ")]
    except ValueError:
        return None
    return tuple(data)
