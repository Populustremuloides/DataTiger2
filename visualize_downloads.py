import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

dir = "C:\\Users\\BCBrown\\Desktop\\TimeSeriesReports"
for file in os.listdir(dir):
    if "time" in file:
        df = pd.read_csv(dir + "\\" + file)

        indices = df["index"]
        pressureHobo = df["pressure_hobo"]
        pressureHanna = df["barometricPressure_hanna"]
        tempHobo = df["temperature_hobo"]

        mask1 = np.asarray(~pressureHobo.isna())
        mask2 = np.asarray(~pressureHanna.isna())

        indices = np.asarray(indices)
        pressureHobo = np.asarray(pressureHobo)
        pressureHanna = np.asarray(pressureHanna)

        indices1 = indices[mask1]
        pressureHobo = pressureHobo[mask1]
        tempHobo = tempHobo[mask1]

        pressureHanna = pressureHanna[mask2]
        indices2 = indices[mask2]

        plt.scatter(x=indices1, y=tempHobo, alpha=0.5)
        plt.title("temperature hobo " + str(file)[-7:-3])
        plt.show()
        plt.scatter(x=indices1, y=pressureHobo, alpha=0.5)
        plt.title("pressure hobo " + str(file)[-7:-3])
        plt.show()
        plt.scatter(x=indices2, y=pressureHanna, alpha=0.5)
        plt.title("pressure hanna" + str(file)[-7:-3])
        plt.show()
        # plot hobo mgl
