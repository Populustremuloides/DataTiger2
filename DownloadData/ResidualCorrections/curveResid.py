from numpy import arange
from numpy import sin
from numpy import sqrt
import numpy as np
import pandas as pd
from scipy.optimize import curve_fit
from matplotlib import pyplot as plt

def objective(x, a, b, c, d, e, f, g, h):
    return (a * x) + (b * x ** 2) + (c * x ** 3) + (d * x ** 4) + (e * x ** 5) + (f * x ** 6) + (g * x ** 7) + h

# longer polynomial but for some reason maxes out after m I think
# def objective(x, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s):
#     return (a * x) + (b * x ** 2) + (c * x ** 3) + (d * x ** 4) + (e * x ** 5) + (f * x ** 6) + (g * x ** 7) + (h * x ** 8) + (i * x ** 9) + (j * x ** 10) + (k * x ** 11) + (l * x ** 12) + (m * x ** 13) + (n * x ** 14) + (o * x ** 15) + (p * x ** 16) + (q * x ** 17) + (r * x ** 18) + s


root = "/Users/zacheliason/Downloads/TimeSeries/timeSeriesReport_CLE.csv"
df = pd.read_csv(root)

fdf = df[~df[f"electricalConductivity_fieldsheet"].isna()]
x = fdf["index"].tolist()
y = fdf.electricalConductivity_fieldsheet.tolist()
minind = x[0]
maxind = x[-1]

popt, _ = curve_fit(objective, x, y)
# a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s = popt
# a, b, c, d, e, f, g, h = popt

# define curve fit to YSI points
# df["YSI_curve"] = objective(df["index"], a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s)
df["YSI_curve"] = objective(df["index"], *popt)

# cut off errant tails of curve
df["YSI_curve"] = df["YSI_curve"].where((df["index"] > minind), None)
df["YSI_curve"] = df["YSI_curve"].where((df["index"] < maxind), None)

# filter df for creating residuals between ysi curve and data points, find residual points
fdf = df[~df[f"electricalConductivity_fieldsheet"].isna()]
fdf = fdf[~fdf[f"electricalConductivity_eureka"].isna()]
fdf = fdf[~fdf[f"YSI_curve"].isna()]
fdf["res_curve"] = [None] * len(fdf)
# fdf["res_curve"] = fdf["YSI_curve"] - fdf["electricalConductivity_eureka"]
fdf["res_curve"] = fdf["electricalConductivity_fieldsheet"] - fdf["electricalConductivity_eureka"]
res = fdf.res_curve.tolist()
x = fdf["index"].tolist()

# add residual points to df
fdf = fdf["res_curve"]
df = pd.concat([df, fdf], axis=1)

# optimize curve for residual points
popt, _ = curve_fit(objective, x, res)

fdf = df[~df["res_curve"].isna()]
x = fdf["index"].tolist()
y = fdf.electricalConductivity_fieldsheet.tolist()
minind = x[0]
maxind = x[-1]

df["residual_curve"] = objective(df["index"], *popt)
df["residual_curve"] = df["residual_curve"].where((df["index"] > minind), None)
df["residual_curve"] = df["residual_curve"].where((df["index"] < maxind), None)

df["corr"] = df["electricalConductivity_eureka"] + df["residual_curve"]

# plot
plt.figure(figsize=(17, 7))
plt.style.use("ggplot")
plt.plot(df["index"], df["electricalConductivity_eureka"], c="yellow", label="original data")
plt.plot(df["index"], df["YSI_curve"],  label="curve fit to YSI points")
plt.plot(df["index"], df["residual_curve"],  label="line fit to residual points")
plt.scatter(df["index"], df["electricalConductivity_fieldsheet"], label="YSI points",  s=3, zorder=10)
plt.scatter(df["index"], df["res_curve"], label="residual points", s=8, zorder=10)
plt.plot(df["index"], df["corr"],  label="corrected data")
plt.legend()
plt.savefig("LOOKHERE.png", dpi=600)