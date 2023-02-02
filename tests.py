from GBS import *
import numpy as np
T = 44/365
amer_implied_vol('c', fs=406.48, x=415, t=T, r=0.0346, q=0.00437, cp=6.33)


print(vol)

div = lambda D, S: np.log(1+D/S)
div(1.781, 406.48)