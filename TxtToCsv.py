# -*- coding: utf-8 -*-
"""
Created on Fri Apr 16 11:32:23 2021

@author: Chase
"""

import pandas as pd

valList=[]
f = open("Haas Data Collection.txt", "r")
for x in f:
  x = x.split(' ')
  if x[1] is not None:
      x_val = x[1]
      valList.append(x_val)
  
print(valList)