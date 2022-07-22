#this file contains old functions from the downLoadTimeSeries.py file that have been replaced, just keeping them to be safe here


# #def getBarometricPressureColumn(siteID, pdf, stationToPriority, output_path, start_date="nan", end_date="nan", save_fig=False):
#     columnPostfix = "_barometricPressure_hanna"
#
#     return pdf[f"{siteID}{columnPostfix}"]
#
#     priorityList = stationToPriority[siteID]
#
#     barometricData = pd.Series([None] * len(pdf[pdf.columns[0]]))
#     barometricDataSites = pd.Series([None] * len(pdf[pdf.columns[0]]))
#
#     mask = np.asarray(barometricData.isna())
#
#     if save_fig:
#         baro_site_means = {}
#         hanna_sites = pdf.columns.tolist()[4:]
#         for site in hanna_sites:
#             baro_site_means[site] = pdf[site].mean()
#
#         variance = pdf[hanna_sites].mean(axis=1)
#
#         plt.figure(figsize=(50, 7))
#         plt.style.use('ggplot')
#         plt.ylabel("Pressure")
#         plt.xlabel("Time")
#
#         plt.plot(variance.index, variance.values, lw=.7, label=f"h(e)")
#
#         for site in hanna_sites:
#             plt.axhline(y=baro_site_means[site], linewidth=.3, label=f"{site} average")
#
#         plt.title(f"Corrected Barometric Pressure {start_date} to {end_date}")
#         plt.legend()
#         plt.clf()
#         plt.close()
#
#     for site in priorityList:
#         columnName = site + columnPostfix
#         siteBarometricData = pdf[columnName]
#         siteBarometricData = np.asarray(siteBarometricData)
#         barometricData[mask] = siteBarometricData[mask]
#         barometricDataSites = barometricDataSites.where(~mask, site)
#         mask = np.asarray(barometricData.isna())
#     barometricData = pd.Series(barometricData)
#     barometricDataSites = barometricDataSites.where(~barometricData.isna(), None)
#
#     bdf = pd.DataFrame({'data': barometricData, 'sites': barometricDataSites})
#     bdf["corrections"] = [0] * len(bdf[bdf.columns[0]])
#     bdf["corrected_values"] = [None] * len(bdf[bdf.columns[0]])
#
#     # chooses how many steps to look forward/backward in determining average correction to apply
#     interval = 2
#
#     values = bdf['sites'].value_counts(dropna=False).keys().tolist()
#     counts = bdf['sites'].value_counts(dropna=False).tolist()
#     value_dict = dict(zip(values, counts))
#
#     ###
#     # finds unique batches and uses them to determine where to apply corrections
#     batch_switches = bdf.drop_duplicates(subset='sites', keep='first')
#     batch_switch_values = batch_switches.sites.values.tolist()
#     bdf["site_changed"] = bdf["sites"].shift(1, fill_value=bdf["sites"].head(1)) != bdf["sites"]
#     bdf['site_changed'] = bdf['site_changed'].where(~bdf['data'].isna(), False)
#     batch_switches = bdf[bdf['site_changed']].index.tolist()
#     # test_dict = dict(zip(batch_switch_values, batch_switches))
#     small_break_indices = []
#
#     batch_differences = np.diff(batch_switches)
#     if None in batch_differences:
#         print("here")
#
#     for i in range(len(batch_differences)):
#         if batch_differences[i] < 4:
#             small_break_indices.append([copy.copy(batch_switches[i]), copy.copy(batch_switches[i + 1])])
#             batch_switches[i] = None
#             batch_switches[i + 1] = None
#
#     batch_switches = [x for x in batch_switches if x is not None]
#
#     ### New force continuity
#     index_switches = batch_switches
#
#     for small in small_break_indices:
#         try:
#             mean_prev = mean(bdf["data"][small[0] - interval:small[0]].tolist())
#             mean_small = mean(bdf['data'][small[0]:small[1]].tolist())
#             diff = mean_prev - mean_small
#             mask = (small[0] >= bdf.index) & (bdf.index <= small[1])
#             bdf['corrections'] = bdf['corrections'].where(~mask, diff)
#             bdf['data'] = bdf['data'] + bdf['corrections']
#             bdf["corrections"] = [0] * len(bdf[bdf.columns[0]])
#         except:
#             print(traceback.format_exc())
#
#     # Looks at previous and next x values of batches, pressure points, and indices
#     for i in range(len(index_switches)):
#         item = index_switches[i]
#         prev_b = bdf["sites"][item - interval:item].tolist()
#         next_b = bdf["sites"][item:item + interval].tolist()
#
#         prev = bdf["data"][item - interval:item].tolist()
#         next = bdf["data"][item:item + interval].tolist()
#
#         prev_i = bdf[item - interval:item].index.tolist()
#         next_i = bdf[item:item + interval].index.tolist()
#
#         # If it's not empty, move forward with comparison
#         if not all([pd.isna(elem) for elem in prev_b]) and not all([pd.isna(elem) for elem in next_b]):
#
#             # Removes na values from previous arrays
#             new_array = []
#             new_indices = []
#             for j in range(len(prev)):
#                 if not pd.isna(prev[j]) and prev[j] != "":
#                     new_array.append(float(prev[j]))
#                     new_indices.append(float(prev_i[j]))
#             prev = new_array
#             prev_i = new_indices
#
#             # Removes na values from next arrays
#             new_array = []
#             new_indices = []
#             for j in range(len(next)):
#                 if not pd.isna(next[j]) and next[j] != "":
#                     new_array.append(float(next[j]))
#                     new_indices.append(float(next_i[j]))
#             next = new_array
#             next_i = new_indices
#
#             # Feel free to correct me here but I reasoned that extreme outliers happening at the batch number switch are unlikely to be valid, thus I throw some away here
#             outliers, outlier_indices = detect_outlier(prev, prev_i)
#             if len(outliers) > 0:
#                 for x in outlier_indices:
#                     bdf.at[x, "pressure_hobo"] = np.nan
#             prev_no_outliers = list(set(prev) - set(outliers))
#             avg_prev = np.nanmean(prev_no_outliers)
#
#             # Feel free to correct me here but I reasoned that extreme outliers happening at the batch number switch are unlikely to be valid, thus I throw some away here
#             outliers, outlier_indices = detect_outlier(next, next_i)
#             if len(outliers) > 0:
#                 for x in outlier_indices:
#                     bdf.at[x, "pressure_hobo"] = np.nan
#             next_no_outliers = list(set(next) - set(outliers))
#             avg_next = np.nanmean(next_no_outliers)
#
#             # Finally, calculate correction value
#             correction = avg_next - avg_prev
#             if pd.isna(correction):
#                 print("OH NO :(")
#             if abs(correction) > 100:
#                 print(f'large correction: {correction}')
#
#             # Apply correction value to all previous pressure points thus reached
#             mask = (bdf.index < index_switches[i])
#             bdf['temp_corrections'] = bdf['corrections'].where(~mask, correction)
#             bdf['corrections'] = bdf['corrections'] + bdf['temp_corrections']
#             bdf["temp_corrections"] = [0] * len(bdf[bdf.columns[0]])
#
#     bdf['corrections'] = bdf['corrections'].where(~bdf['data'].isna(), None)
#     bdf['corrected_values'] = pd.to_numeric(bdf['corrections']) + pd.to_numeric(bdf['data'])
#
#     if save_fig:
#
#         # ############################
#         # TEST CONTINUITY BY PLOTTING
#         # ############################
#
#         plt.figure(figsize=(50, 7))
#         plt.style.use('ggplot')
#         plt.ylabel("Pressure")
#         plt.xlabel("Time")
#
#         plt.plot(bdf.index, bdf["data"], lw=.3, zorder=2, c='grey', linestyle='dotted', label=f"original data")
#         # plt.plot(bdf.index, bdf["corrected_values"], lw=.3, zorder=2, label=f"corrected")
#
#         groups = bdf.groupby('sites')
#         for name, group in groups:
#             plt.scatter(x=group.index, y=group.corrected_values, s=3, zorder=4, label=f"{name}")
#
#         plt.title(f"Corrected Barometric Pressure {start_date} to {end_date}")
#         plt.legend()
#         plt.savefig(f"{output_path}/{siteID}/barometric_pressure_corrected_{start_date}_to_{end_date}.png", dpi=300)
#         plt.clf()
#         plt.close()
#
#         old_output_path = copy.copy(outputPath)
#         if not os.path.isdir(os.path.join(outputPath, siteID)):
#             os.mkdir(os.path.join(outputPath, siteID))
#         outputPath = old_output_path
#
#     # Old force continuity:
#     # for i in range(len(index_switches)):
#     #     item = index_switches[i]
#     #     prev_b = bdf["sites"][item - interval:item].tolist()
#     #     next_b = bdf["sites"][item:item + interval].tolist()
#     #
#     #     prev = bdf["data"][item - interval:item].tolist()
#     #     next = bdf["data"][item:item + interval].tolist()
#     #
#     #     if not all([elem is None for elem in prev_b]) and not all([elem is None for elem in next_b]):
#     #         prev = [float(pressure) for pressure in prev if pressure is not None and pressure != ""]
#     #         next = [float(pressure) for pressure in next if pressure is not None and pressure != ""]
#     #
#     #         outliers = detect_outlier(prev)
#     #         if len(outliers) > 0:
#     #             print(f'help {outliers}')
#     #             pass
#     #
#     #         prev_no_outliers = list(set(prev) - set(outliers))
#     #         avg_prev = mean(prev_no_outliers)
#     #
#     #         outliers = detect_outlier(next)
    #         if len(outliers) > 0:
    #             print(f'help {outliers}')
    #             pass
    #
    #         next_no_outliers = list(set(next) - set(outliers))
    #         avg_next = mean(next_no_outliers)
    #
    #         correction_this_site = avg_prev - avg_next
    #         correction = correction_this_site + cumulative_correction
    #
    #         if abs(correction) > 100:
    #             print(f'large correction: {correction_this_site}')
    #
    #         mask = (bdf.index >= index_switches[i - 1]) & (bdf.index < index_switches[i])
    #         bdf['corrections'] = bdf['corrections'].where(~mask, correction)
    #         cumulative_correction = cumulative_correction + correction_this_site

    # bdf['corrections'] = bdf['corrections'].where(~bdf['data'].isna(), None)
    # bdf['corrected_values'] = pd.to_numeric(bdf['corrections']) + pd.to_numeric(bdf['data'])

    # barometricData = bdf['corrected_values']
    # barometricData = bdf['corrected_values']
    # return barometricData, bdf







#old getDischargeToPressureDF function
# def getDischargeToPressureDF(df, siteID, pdf, cursor, output_path, start_date, end_date):
#     # barometricData, bdf = getBarometricPressureColumn(siteID, pdf, stationToPriority, output_path, start_date, end_date, True)
#     try:
#         barometricData = pdf[f"{siteID}_barometricPressure_hanna"]
#     except:
#         print(traceback.format_exc())
#         print('oaky')

    # step is equal to the difference in index equivalent to 3 hrs (12 indices == 12 15 min intervals == 3 hrs).
    #step = 12

    # # drop None values in pressure_hobo
    # indices_no_na = pd.Series(barometricData[~barometricData.isna()].index)
    #
    # # create series of differences between indices so we can later find the jumps where Nones were taken out
    # differences = indices_no_na.diff()
    # # create series of indices of aforementioned differences ^
    # index_df = differences.index
    # # create dict and then df from series
    # d = {'differences': differences, 'index_df': index_df}
    # idf = pd.DataFrame(d)
    #
    # # starts == the beginning index of each individual grouping
    # starts = idf.loc[(idf['differences'] >= step)]['index_df'].values.tolist()
    #
    # if len(starts) > 0:
    #     print("we have a huge err that I don't have the bandwidth to troubleshoot rn sorry :)")
    #     print("basically you have jumps within the barometric pressure data, idk maybe not a huge deal")

    # barometric_pairings = [barometricData.loc[indices_no_na.values.tolist()[0]]['index'], barometricData.loc[indices_no_na.values.tolist()[-1]]['index']]

    # plt.figure(figsize=(50, 7))
    # plt.style.use('ggplot')
    # plt.ylabel("pressure")
    # # plt.xlabel(f"{stationToPriority[siteID]}")
    #
    # # Coloring biz
    # color_labels = bdf['sites'].unique()
    # rgb_values = sns.color_palette("Set2", 8)
    # color_map = dict(zip(color_labels, rgb_values))
    # df = df.reset_index()
    # bdf['colors'] = bdf['sites'].map(color_map)
    #
    # groups = bdf.groupby('sites')
    # for name, group in groups:
    #     plt.scatter(x=group.index, y=group.data, s=3, zorder=4, label=f"{name}")
    #
    # # plt.scatter(x=bdf.index, y=bdf['data'], c=bdf['colors'], lw=.4, label=bdf["sites"])
    # # plt.scatter(x=df.index, y=df["discharge_measured"], c="tomato", s=5)
    #
    # plt.legend()
    # plt.clf()
    # plt.close()

    # columnPostfix = "_barometricPressure_hanna"
    # priorityList = stationToPriority[siteID]
    #
    # pdfMask = list(~df["discharge_measured"].isna())
    # if len(pdfMask) != len(pdf):
    #     if len(pdfMask) < len(pdf):
    #         pdfMask.append(False)
    #     elif len(pdfMask) > len(pdf):
    #         pdfMask = pdfMask[:-1]
    #
    # try:
    #     dischargeIndices = pdf[pdfMask]["index"]
    #
    #     # do for barometric pressure
    #
    #     correctedPressurePoints = df[~df["discharge_measured"].isna()]["corrected_values"]
    #     correctionPoints = df[~df["discharge_measured"].isna()]["corrections"]
    #     pressurePoints = df[~df["discharge_measured"].isna()]["pressure_hobo"]
    #     pressureData = df["pressure_hobo"]
    #     correctedPressureData = df["corrected_values"]
    #     dischargePoints = df[~df["discharge_measured"].isna()]["discharge_measured"]
    #     datePoints = df[~df["discharge_measured"].isna()]["datetime"]
    #
    # except:
    #     print(traceback.format_exc())
    #     print("tears")
    #
    # xs = []
    # ys = []
    # zs = []
    # dates = []
    #
    # press = []
    # corrected_press = []
    # corrections_short = []
    # corrections_full = []
    # barPress = []
    # dis = []
    # fullDates = []
    # for i in range(len(dischargeIndices)):
    #     index = list(dischargeIndices)[i]
    #     pressure = list(pressurePoints)[i]
    #     discharge = list(dischargePoints)[i]
    #     date = list(datePoints)[i]
    #     corrected_pressure = list(correctedPressurePoints)[i]
    #     correction = list(correctionPoints)[i]
    #
    #     expandedIndex = expandIndex(index, df["index"])
    #     expandedIndex = expandedIndex[:len(pdf[pdf.columns[0]])]
    #
    #     # because "" values can be in there instead of None values
    #     nearbyBarometricMeasurements = barometricData[expandedIndex]
    #     nearbyPressureMeasurements = pressureData[:len(barometricData)][expandedIndex]
    #     nearbyCorrectedPressureMeasurements = correctedPressureData[:len(barometricData)][expandedIndex]
    #
    #     nearbyBarometricMeasurements = replaceBlankWithNone(nearbyBarometricMeasurements)
    #     nearbyPressureMeasurements = replaceBlankWithNone(nearbyPressureMeasurements)
    #     nearbyCorrectedPressureMeasurements = replaceBlankWithNone(nearbyCorrectedPressureMeasurements)
    #
    #     nearbyBarometricMeasurements = pd.Series(nearbyBarometricMeasurements)
    #     nearbyPressureMeasurements = pd.Series(nearbyPressureMeasurements)
    #     nearbyCorrectedPressureMeasurements = pd.Series(nearbyCorrectedPressureMeasurements)
    #
    #     maskB = np.asarray(~nearbyBarometricMeasurements.isna())
    #     maskP = np.asarray(~nearbyPressureMeasurements.isna())
    #     maskC = np.asarray(~nearbyCorrectedPressureMeasurements.isna())
    #
    #     nearbyBarometricMeasurements = np.asarray(nearbyBarometricMeasurements)
    #     nearbyPressureMeasurements = np.asarray(nearbyPressureMeasurements)
    #     nearbyCorrectedPressureMeasurements = np.asarray(nearbyCorrectedPressureMeasurements)
    #
    #     if np.sum(maskB) > 0:
    #         meanBarometricPressure = np.mean(nearbyBarometricMeasurements[maskB])
    #     else:
    #         meanBarometricPressure = None
    #
    #     if np.sum(maskP) > 0:
    #         meanPressure = np.mean(nearbyPressureMeasurements[maskP])
    #     else:
    #         meanPressure = None
    #
    #     if np.sum(maskC) > 0:
    #         meanCorrectedPressure = np.mean(nearbyCorrectedPressureMeasurements[maskC])
    #     else:
    #         meanCorrectedPressure = None
    #
    #     press.append(meanPressure)
    #     corrected_press.append(meanCorrectedPressure)
    #     barPress.append(meanBarometricPressure)
    #     dis.append(discharge)
    #     fullDates.append(date)
    #     corrections_full.append(correction)
    #
    #     if meanBarometricPressure != None and meanCorrectedPressure != None and discharge != None:
    #         if not np.isnan(meanBarometricPressure) and not np.isnan(meanPressure) and not np.isnan(discharge):
    #             discounted_pressure_point = float(meanPressure) - float(meanBarometricPressure)
    #             if discounted_pressure_point > -15:
    #                 xs.append(discounted_pressure_point)
    #                 zs.append(float(meanCorrectedPressure) - float(meanBarometricPressure))
    #                 ys.append(discharge)
    #                 dates.append(date)
    #                 corrections_short.append(correction)
    #             else:
    #                 print('err')
    #
    # returnDict = {"barometric_discounted_original_pressure": xs, "barometric_discounted_corrected_pressure": zs, "measured_discharge":ys, "datetime":dates, "corrections": corrections_short}
    # longDict = {"barometricPressure": barPress, "absolutePressure": press,"discharge": dis, "datetime": fullDates, "correctedPressure": corrected_press, "corrections": corrections_full}
    # # FIXME: this isn't quite getting it right!
    #
    # returnDF = pd.DataFrame.from_dict(returnDict)
    # longDF = pd.DataFrame.from_dict(longDict)
    #
    # if len(returnDF.index) == 0 and len(longDF.index) == 0:
    #     return None, None
    #
    # return returnDF, longDF






    #
    # cols = []
    #     for point_sensor in resDict.keys():
    #         for target in target_list:
    #             df[f"{point_sensor}_corrected_{target}"] = [None] * len(df[df.columns[0]])
    #             df[f"{point_sensor}_residual_{target}"] = [None] * len(df[df.columns[0]])
    #             cols.extend([f"{point_sensor}_residual_{target}", f"{point_sensor}_corrected_{target}"])
    #             if f"{target}_fieldsheet" not in cols and f"{target}_{device}" not in cols:
    #                 cols.extend([f"{target}_fieldsheet", f"{target}_{device}"])

# def correctValues(df, sensors, target_list):
#     df = df.replace("", np.nan, regex=True)
#     df = df.fillna(value=np.nan)
#
#     resDict, device, df, target_list = loadResidualDict(df, target_list, sensors)
#     if device is None:
#         return None, None
#
#     # specify only important columns for final export
#     cols = []
#     for point_sensor in resDict.keys():
#         for target in target_list:
#             df[f"{point_sensor}_corrected_{target}"] = [None] * len(df[df.columns[0]])
#             df[f"{point_sensor}_residual_{target}"] = [None] * len(df[df.columns[0]])
#             cols.extend([f"{point_sensor}_residual_{target}", f"{point_sensor}_corrected_{target}"])
#             if f"{target}_fieldsheet" not in cols and f"{target}_{device}" not in cols:
#                 cols.extend([f"{target}_fieldsheet", f"{target}_{device}"])
#
#     # for each row in df, point_sensor, and target, calculate necessary values based on preloaded line formula dictionary
#     nan_df = pd.isnull(df)
#     for index, row in df.iterrows():
#         for point_sensor in resDict.keys():
#             for target in resDict[point_sensor].keys():
#                 for k, v in resDict[point_sensor][target].items():
#                     if index < k[0]:
#                         pass
#                     elif k[0] <= index < k[1]:
#                         # calculate corrected position of current index using point-slope formula from piecewise residual function
#                         y = (v["m"] * (index - v["x1"])) + v["y1"]
#                         df.at[index, f"{point_sensor}_residual_{target}"] = y
#                         try:
#                             if not nan_df.at[index, f"{target}_{device}"]:
#                                 df.at[index, f"{point_sensor}_corrected_{target}"] = float(
#                                     df.at[index, f"{target}_{device}"]) + y
#                         except:
#                             print(traceback.format_exc())
#                             print(f"{target}_{device}")
#                         # if current index lies within range, continue to subsequent targets
#                     elif index >= k[1]:
#                         # it'd be nice for performance' sake to not have to cycle through each visited key after it's been passed but I can't think of a quick fix that works at the moment
#                         # following doesn't work
#                         pass
#
#     return df, device


# def loadResidualDict(df, target_list, sensors):
#     point_slope_dict = {}
#
#     # sense device type
#     device = senseDeviceType(target_list, df)
#
#     # correct df names based on device type
#     if device == "hobo":
#         df = df.rename(columns={"conductivity_hobo": "electricalConductivity_hobo", "dissolvedOxygen_mgl_hobo": "dissolvedOxygen_mgL_hobo"})
#         target_list.remove("orpMV")
#         target_list.remove("pH")
#     elif device == "eureka":
#         df = df.rename(columns={"orp_eureka": "orpMV_eureka"})
#         target_list.remove("dissolvedOxygen_mgL")
#     elif device == "hanna":
#         target_list.append("dissolvedOxygenPercent")
#     else:
#         return None, None, None, None
#
#     for point_sensor in sensors:
#         point_slope_dict[point_sensor] = {}
#         filtered_df = df[~df[f"{target_list[0]}_fieldsheet"].isna()]
#         filtered_df = filtered_df[filtered_df['device'].str.contains(f"{point_sensor}", na=False)]
#
#         for target in target_list:
#             # further filter df to include only entries with valid readings
#             filter_by_target = filtered_df[~filtered_df[f"{target}_{device}"].isna()]
#             df[f"{point_sensor}_{target}_fieldsheet"] = df[f"{target}_fieldsheet"].mask((~df['device'].str.contains(f"{point_sensor}", na=True)), other=None)
#
#             point_slope_dict[point_sensor][target] = {}
#
#             filter_by_target.loc[:, f"{point_sensor}_{target}_residual_line"] = [None] * len(filter_by_target.index)
#
#             x1, y1, m = None, None, None
#             for index, row in filter_by_target.iterrows():
#                 try:
#                     tf = row[f"{target}_fieldsheet"]
#                     # Error check type etc!
#                     if type(tf) == str:
#                         if ":" in tf:
#                             tf = tf.replace(":", ".")
#                             df.at[index, f"{target}_fieldsheet"] = tf
#                         if tf == "-" or "0($0 !11&8$" in tf or r"435.\x01$0$" in tf:
#                             tf == None
#                         else:
#                             tf = float(tf)
#                     elif type(tf) == int:
#                         tf = float(tf)
#                     elif type(tf) != float:
#                         if tf is None:
#                             tf = np.nan
#                         else:
#                             print(f"ERR: type {type(tf)}")
#                     if not math.isnan(tf):
#                         if type(row[f"{target}_{device}"]) == str:
#                             if row[f"{target}_{device}"] == "":
#                                 print("help!")
#                         filter_by_target.at[index, f"{target}_{device}"] = float(row[f"{target}_{device}"])
#                         # if not first iteration
#                         if x1 is not None:
#                             # calculate slope
#                             m = ((tf - float(row[f"{target}_{device}"]) - y1) / (index - x1))
#                             # from points x1 - x2, slope is m with y1 of y1
#                             point_slope_dict[point_sensor][target][(x1, index)] = {"x1": x1, "y1": y1, "m": m}
#                         y1 = tf - float(row[f"{target}_{device}"])
#                         x1 = index
#                 except:
#                     print(traceback.format_exc())
#                     print("could not convert", row[f"{target}_fieldsheet"], "on target:", target)
#
#     return point_slope_dict, device, df, target_list
