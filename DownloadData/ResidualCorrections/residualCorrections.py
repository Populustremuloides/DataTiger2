import pandas as pd
import numpy as np
import math
import os
import matplotlib.pyplot as plt
import traceback
import copy

def loop_over(point_sensor, target, index, device, nan_df):
    for k, v in resDict[point_sensor][target].items():
        if index < k[0]:
            pass
            # if current index less than lower bound of range, break to next index instead of to subsequent targets
            # return True
        elif k[0] <= index < k[1]:
            # calculate corrected position of current index using point-slope formula from piecewise residual function
            y = (v["m"] * (index - v["x1"])) + v["y1"]
            df.at[index, f"{point_sensor}_residual_{target}"] = y
            try:
                if not nan_df.at[index, f"{target}_{device}"]:
                    df.at[index, f"{point_sensor}_corrected_{target}"] = float(df.at[index, f"{target}_{device}"]) + y
            except:
                print(traceback.format_exc())
                print(f"{target}_{device}")
            # if current index lies within range, continue to subsequent targets
        elif index >= k[1]:
            # it'd be nice for performance' sake to not have to cycle through each visited key after it's been passed but I can't think of a quick fix that works at the moment
            # following doesn't work
            del resDict[point_sensor][target][k]
        return False

def senseDeviceType(target_list, df):
    parse_df = df[~df[f"{target_list[0]}_fieldsheet"].isna()]
    if not all(parse_df["conductivity_hobo"].isna()):
        device = "hobo"
    elif not all(parse_df["electricalConductivity_eureka"].isna()):
        device = "eureka"
    elif not all(parse_df["electricalConductivity_hanna"].isna()):
        device = "hanna"
    else:
        device = ""
        print("HUGE ERR: no device detected :(")
        print("see whether you can trace the error?")
    return device

def loadResidualDict(df, target_list, filename, sensors):
    point_slope_dict = {}

    # sense device type
    device = senseDeviceType(target_list, df)

    # correct df names based on device type
    if device == "hobo":
        df = df.rename(columns={"conductivity_hobo": "electricalConductivity_hobo", "dissolvedOxygen_mgl_hobo": "dissolvedOxygen_mgL_hobo"})
        target_list.remove("orpMV")
        target_list.remove("pH")
    elif device == "eureka":
        df = df.rename(columns={"orp_eureka": "orpMV_eureka"})
        target_list.remove("dissolvedOxygen_mgL")
    elif device == "hanna":
        target_list.append("dissolvedOxygenPercent")
    else:
        return None, None, None, None

    for point_sensor in sensors:
        point_slope_dict[point_sensor] = {}
        filtered_df = df[~df[f"{target_list[0]}_fieldsheet"].isna()]
        filtered_df = filtered_df[filtered_df['device'].str.contains(f"{point_sensor}", na=False)]


        for target in target_list:
            # further filter df to include only entries with valid readings
            filter_by_target = filtered_df[~filtered_df[f"{target}_{device}"].isna()]
            df[f"{point_sensor}_{target}_fieldsheet"] = df[f"{target}_fieldsheet"].mask((~df['device'].str.contains(f"{point_sensor}", na=True)), other=None)

            point_slope_dict[point_sensor][target] = {}
            # filtered_df[f"corrected_{target}"] = [None] * len(filtered_df[filtered_df.columns[0]])
            filter_by_target[f"{point_sensor}_{target}_residual_line"] = [None] * len(filter_by_target.index)

            x1, y1, m = None, None, None
            for index, row in filter_by_target.iterrows():
                try:
                    tf = row[f"{target}_fieldsheet"]
                    # Error check type etc!
                    if type(tf) == str:
                        if ":" in tf:
                            tf = tf.replace(":", ".")
                            df.at[index, f"{target}_fieldsheet"] = tf
                        tf = float(tf)
                    elif type(tf) != float:
                        print("OH NO!")
                    if not math.isnan(tf):
                        # if not first iteration
                        if x1 is not None:
                            # calculate slope
                            m = ((tf - float(row[f"{target}_{device}"]) - y1) / (index - x1))
                            # from points x1 - x2, slope is m with y1 of y1
                            point_slope_dict[point_sensor][target][(x1, index)] = {"x1": x1, "y1": y1, "m": m}
                        y1 = tf - float(row[f"{target}_{device}"])
                        x1 = index
                except:
                    print(traceback.format_exc())
                    print("could not convert", row[f"{target}_fieldsheet"], "to float at site", filename[-7:-4], "on target:", target)

    return point_slope_dict, device, df, target_list


sensors = ["Hanna", "YSI"]
root = "/Users/zacheliason/Downloads/TimeSeries/"
progressList = os.listdir(root)
for filename in os.listdir(root):
    # save time
    # if not os.path.isfile(f"{os.getcwd()}/{filename[-7:-4]}_electricalConductivity.png"):
    # if filename[-7:-4] == "CLE":
        print(f"Now working on {filename[-7:-4]}, {len(progressList)} more to go!")
        # open file
        df = pd.read_csv(root + filename)
        target_list = ["electricalConductivity", "pH", "temperature", "orpMV", "dissolvedOxygen_mgL"]
        resDict, device, df, target_list = loadResidualDict(df, target_list, filename, sensors)
        if device is None:
            print(f"corrections for {filename[-7:-4]} failed, inspect further :(")
            continue

        # specify only important columns for final export
        cols = []
        for point_sensor in resDict.keys():
            for target in target_list:
                df[f"{point_sensor}_corrected_{target}"] = [None] * len(df[df.columns[0]])
                df[f"{point_sensor}_residual_{target}"] = [None] * len(df[df.columns[0]])
                cols.extend([f"{point_sensor}_residual_{target}", f"{point_sensor}_corrected_{target}"])
                if f"{target}_fieldsheet" not in cols and f"{target}_{device}" not in cols:
                    cols.extend([f"{target}_fieldsheet", f"{target}_{device}"])

        # for each row in df, point_sensor, and target, calculate necessary values based on preloaded line formula dictionary
        nan_df = pd.isnull(df)
        for index, row in df.iterrows():
            for point_sensor in resDict.keys():
                for target in resDict[point_sensor].keys():
                    if loop_over(point_sensor, target, index, device, nan_df):
                        break

        for target in target_list:
            try:
                # save figure
                plt.figure(figsize=(17, 7))
                plt.style.use('ggplot')
                plt.xlabel("Days Since October 1, 2018")
                plt.ylabel(f"{target}")
                plt.plot(df["index"], df[f"{target}_{device}"], c="grey", lw=.5, zorder=1, label="raw data")
                for point_sensor in sensors:
                    if point_sensor == "Hanna":
                        plt.plot(df["index"], df[f"{point_sensor}_residual_{target}"], lw=1, ls="dotted", c="coral", zorder=1, label=f"{point_sensor} residual")
                        plt.plot(df["index"], df[f"{point_sensor}_corrected_{target}"], lw=.5, c="orange", zorder=1, label=f"{point_sensor} corrected values")
                        plt.scatter(df["index"], df[f"{point_sensor}_{target}_fieldsheet"], s=2, c="orangered", zorder=2, label=f"{point_sensor} fieldsheet values")
                    else:
                        plt.plot(df["index"], df[f"{point_sensor}_residual_{target}"], lw=1, ls="dotted", c="cornflowerblue", zorder=2, label=f"{point_sensor} residual")
                        plt.plot(df["index"], df[f"{point_sensor}_corrected_{target}"], lw=1, c="darkcyan", zorder=3, label=f"{point_sensor} corrected values")
                        plt.scatter(df["index"], df[f"{point_sensor}_{target}_fieldsheet"], s=6, c="indigo", zorder=4, label=f"{point_sensor} fieldsheet values")
                plt.axvline(x=945, c="tomato", zorder=7, label="stopped calibrating May 3, 2021")
                plt.title(f"{filename[-7:-4]} {target}")
                plt.legend()
                plt.savefig(f"{filename[-7:-4]}_{target}.png", dpi=300)
                plt.clf()
                plt.close()
            except:
                print(traceback.format_exc())
                print("failed PNG export!!", "point sensor:", point_sensor, "and target:", target)

        df = df[cols]
        df.to_csv(f"{filename[-7:-4]}_residual.csv")
        progressList.remove(filename)