import tkinter as tk
from tkinter import filedialog
import pandas as pd
import easygui

path = easygui.fileopenbox()
print(path)
# root = tk.Tk()
# root.withdraw()
#
# file_path = filedialog.askopenfilename()
Q_codes = pd.read_excel(path)
print(Q_codes)