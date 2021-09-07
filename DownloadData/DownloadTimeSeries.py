import sqlite3
from scipy.optimize import curve_fit
from scipy import stats

import copy
from statistics import *
import platform
from DownloadData.SQLQueries import *
import pandas as pd
from DownloadData.DateToIndex import *
import numpy as np
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
import traceback
import math
import os

def getIndexList():
    # go from the start date to now
    now = str(datetime.now())
    date, time = now.split(" ")
    year, month, day = date.split("-")
    year = year[2:]
    hour, minute, second = time.split(":")
    second = second[:2]

    endIndex = datetimeToIndex(year, month, day, hour, minute, second)
    endIndex = round(endIndex / dayToIndexRatio) * dayToIndexRatio

    diff = endIndex - startIndex
    numIndices = diff / dayToIndexRatio

    indexList = []
    for i in range(0,int(numIndices)):
        newVal = startIndex + (i * (dayToIndexRatio))
        indexList.append(newVal)

    return indexList


def joinDictSite(dict1, fullDict, siteID):
    dataNames = dict1.keys()
    for name in dataNames:
        if name == "index" or name == "datetime":
            pass
        else:

            dataList = dict1[name]
            indices = dict1["index"]

            newData = [None] * len(fullDict["index"])

            for i in range(len(indices)):
                index = indices[i]
                #listIndex = int(index / dayToIndexRatio) - int(startIndex / dayToIndexRatio)
                listIndex = round(index / dayToIndexRatio)

                data = dataList[i]

                newData[listIndex] = data

            newName = siteID + "_" + name
            fullDict[newName] = newData
    return fullDict


def joinDict(dict1, fullDict):
    
    dataNames = dict1.keys()
    for name in dataNames:
        if name == "index" or name == "datetime":
            pass

        elif name == "batch_id" and "batch_id" in fullDict.keys() and len(fullDict["batch_id"]) == len(fullDict["index"]):
            pass
            # dataList = dict1[name]
            # indices = dict1["index"]
            #
            # for i in range(len(indices)):
            #
            #     batch_id = dataList[i]
            #     index = indices[i]
            #     # listIndex = int(index / dayToIndexRatio) - int(startIndex / dayToIndexRatio)
            #     listIndex = round(index * indexToDayRatio)
            #
            #     fullDict["batch_id"][listIndex] = batch_id
        else:
            dataList = dict1[name]
            indices = dict1["index"]
            dtime = dict1["datetime"]

            # if len(indices) > 0:
            #     if max(indices) > 646.6:
            #         for i in range(len(indices)):
            #             if indices[i] > 646.6:
            #                 print("*******************************************************************************")
            #             print(indices[i])
            #             print(dtime[i])

            newData = [None] * len(fullDict["index"])
            # print(fullDict["datetime"][-5:])
            for i in range(len(indices)):
               # print(dtime[i])
               # print(index)
               # print(indexToDayRatio)
               # print((indexToDayRatio * indices[-1]) - (indexToDayRatio * indices[0]))
               # print(index / dayToIndexRatio)
               # print(index * dayToIndexRatio)
                #print(len(newData))

                index = indices[i]
                #listIndex = int(index / dayToIndexRatio) - int(startIndex / dayToIndexRatio)
                listIndex = round(index * indexToDayRatio)

                data = dataList[i]
                dt = dtime[i]
                newData[listIndex] = data

            fullDict[name] = newData
    return fullDict

def getDateList(indexList):
    dateList = []
    for index in indexList:
        year, month, day, hour, minute, second = indexToDatetime(index, startYear)
        datetime = str(year) + "-" + str(month) + "-" + str(day) + " " + str(hour) + ":" + str(minute) + ":" + str(second)
        dateList.append(datetime)
    return dateList

def getWaterYearFromDate(date):
    date, time = date.split(" ")
    year, month, date = date.split("-")
    year = int(year)
    month = int(month)
    if month >= 10:
        year += 1
    return year

def getIndexInWaterYearList(dateList, indexList):
    if len(dateList) > 0:
        indexInWaterYearList = []
        waterYears = []
        previousWaterYear = getWaterYearFromDate(dateList[0])
        subractionValue = 0
        for i in range(len(indexList)):
            date = dateList[i]
            index = indexList[i]
            waterYear = getWaterYearFromDate(date)

            if waterYear != previousWaterYear:
                if previousWaterYear % 4 == 0:
                    numDaysInYear = 366
                else:
                    numDaysInYear = 365
                subractionValue += numDaysInYear

            indexInWaterYearList.append(index - subractionValue)
            waterYears.append(waterYear)

            previousWaterYear = waterYear
    return indexInWaterYearList, waterYears

def makeSiteDF(cursor, siteID, nbsNum, citSciNum, testsDict, optionsDict):
    indexList = getIndexList()
    dateList = getDateList(indexList)
    indexInWaterYear, waterYear = getIndexInWaterYearList(dateList, indexList)

    # print("dates")
    # print(dateList[-50:])
    # print("indices")
    # print(indexList[-50:])
    fullDict = {
        "index": indexList,
        "indexInWaterYear":indexInWaterYear,
        "waterYear":waterYear,
        "datetime": dateList
    }
    # fullDf = pd.DataFrame.from_dict(fullDict)
    # fullDf.to_csv("lookatme.csv")

    if testsDict["fieldSheetInfo"]:
        # try:
            fieldSheetDict = getFieldSheetInfo(cursor, siteID, nbsNum, citSciNum)
            fullDict = joinDict(fieldSheetDict, fullDict)
        # except:
        #     print("ERROR")
    if testsDict["hoboPressure"] or optionsDict["calculateDischarge"]:
        # try:
            pDict = getP(cursor, siteID)
            fullDict = joinDict(pDict, fullDict)
        # except:
        #     print("ERROR")
    if testsDict["hoboLight"]:
        # try:
            lightDict = getLightHobo(cursor, siteID)
            fullDict = joinDict(lightDict, fullDict)
        # except:
        #     print("error")
    if testsDict["hoboConductivity"]:
        # try:
            condDict = getConductivityHobo(cursor, siteID)
            fullDict = joinDict(condDict, fullDict)
        # except:
        #     print("error")
    if testsDict["hoboOxygen"]:
        # try:
            oxygenDict = getOxygenHobo(cursor, siteID)
            fullDict = joinDict(oxygenDict, fullDict)
        # except:
        #     print("error")
    if testsDict["measuredDischarge"]:
        # try:
            qDict = getQ(cursor, siteID)
            fullDict = joinDict(qDict, fullDict)
        # except:
        #     print("error")
    if testsDict["hanna"]:
        try:
            hannaDict = getHanna(cursor, siteID)
            fullDict = joinDict(hannaDict, fullDict)
        except:
            print("error")
    if testsDict["eureka"]:
        # try:
            eurekaDict = getEureka(cursor, siteID)
            fullDict = joinDict(eurekaDict, fullDict)
        # except:
        #     print("error")
    if testsDict["elementar"]:
        # try:
            elementarDict = getElementar(cursor, siteID, nbsNum, citSciNum)
            fullDict = joinDict(elementarDict, fullDict)
        # except:
        #     print("error")
    if testsDict["scanCalculated"]:
        # try:
            scanParDict = getScanPar(cursor, siteID, nbsNum, citSciNum)
            fullDict = joinDict(scanParDict, fullDict)
        # except:
        #     print("error")
    if testsDict["scanRaw"]:
        # try:
            scanFPDict = getScanFp(cursor, siteID, nbsNum, citSciNum)
            fullDict = joinDict(scanFPDict, fullDict)
        # except:
        #     print("error")
    if testsDict["ic"]:
        # try:
            icCationDict = getICCation(cursor, siteID, nbsNum, citSciNum)
            fullDict = joinDict(icCationDict, fullDict)
        # except:
        #     print("error")
    if testsDict["ic"]:
        # try:
            icAnionDict = getICAnion(cursor, siteID, nbsNum, citSciNum)
            fullDict = joinDict(icAnionDict, fullDict)
        # except:
        #     print("error")
    if testsDict["icp"]:
        # try:
            icpDict = getICP(cursor, siteID, nbsNum, citSciNum)
            fullDict = joinDict(icpDict, fullDict)
        # except:
        #     print("error")
        
    df = pd.DataFrame.from_dict(fullDict)
    if optionsDict["include_batch_id"]:
        pass
    elif "batch_id" in df.columns:
        df = df.drop("batch_id", axis=1)
        ###
    return df

def saveDF(df, outputPath, siteId, nbsNum, saveFig=False, corrections_df=None, target_list=None, sensors=None, device=None):
    if platform.system() == "Windows":
        if siteID != "":
            filePath = outputPath + "\\timeSeriesReport_" + siteId + ".csv"
            figPath = outputPath + "\\timeSeriesCorrectionFigures\\" + siteId
            figTitle = siteId
        else:
            filePath = outputPath + "\\timeSeriesReport_NBS" + nbsNum + ".csv"
            figPath = outputPath + "\\timeSeriesCorrectionFigures_NBS\\" + nbsNum
            figTitle = f"NBS_{nbsNum}"
    else:
        if siteId != "":
            filePath = outputPath + "/timeSeriesReport_" + siteId + ".csv"
            figPath = outputPath + "/timeSeriesCorrectionFigures/" + siteId
            figTitle = siteId
        else:
            filePath = outputPath + "/timeSeriesReport_NBS" + nbsNum + ".csv"
            figPath = outputPath + "/timeSeriesCorrectionFigures_NBS/" + nbsNum
            figTitle = f"NBS_{nbsNum}"

    print(filePath)
    df.to_csv(filePath, index=False)
    if saveFig:
        saveFigure(corrections_df, figPath, target_list, sensors, figTitle, device)

def saveFigure(df, figPath, target_list, sensors, figTitle, device):
   for target in target_list:
        try:
            # save figure
            plt.figure(figsize=(17, 7))
            plt.style.use('ggplot')
            plt.xlabel("Days since 2018")
            plt.ylabel(f"{target}")
            plt.scatter(df["index"], df[f"{target}_{device}"], c="grey", s=.5, zorder=1, label="raw data")


            # for point_sensor in sensors:
            #     if point_sensor == "Hanna":
            #         plt.plot(df["index"], df[f"{point_sensor}_residual_{target}"], lw=1, ls="dotted", c="coral",
            #                  zorder=1, label=f"{point_sensor} residual")
            #         plt.plot(df["index"], df[f"{point_sensor}_corrected_{target}"], lw=.5, c="orange", zorder=1,
            #                  label=f"{point_sensor} corrected values")
            #         plt.scatter(df["index"], df[f"{point_sensor}_{target}_fieldsheet"], s=2, c="orangered", zorder=2,
            #                     label=f"{point_sensor} fieldsheet values")
            #     else:


            # THESE ARE REAL!
            # plt.plot(df["index"], df[f"YSI_curve_{target}"], lw=1, ls="dotted",
            #          c="cornflowerblue", zorder=2, label=f"residual_curve_{target}")
            # plt.plot(df["index"], df[f"residual_curve_{target}"], lw=1, c="darkcyan", zorder=3,
            #          label=f"residual_curve_{target}")
            # plt.plot(df["index"], df[f"{target}_{device}_corrected"], lw=1, c="indigo", zorder=4,
            #             label=f"{target}_{device}_corrected")
            # plt.scatter(df["index"], df[f"{target}"], s=6, c="green", zorder=4,
            #             label=f"residual points")
            plt.scatter(df["index"], df[f"{target}_fieldsheet"], s=6, c="tomato", zorder=4,
                        label=f"{target}_YSI")
            plt.axvline(x=945, c="tomato", zorder=7, label="Stopped calibrating May 3, 2021")
            plt.title(f"{figTitle} {target}")
            plt.legend()
            plt.savefig(f"{figPath}_{target}.png", dpi=300)
            plt.clf()
            plt.close()
        except:
            print(traceback.format_exc())
            print(f"PNG export for {figTitle} failed at target: {target}")

def getAllHannaPressuresDF(cursor):
    indexList = getIndexList()
    dateList = getDateList(indexList)
    indexInWaterYear, waterYear = getIndexInWaterYearList(dateList, indexList)

    fullDict = {
        "index": indexList,
        "indexInWaterYear": indexInWaterYear,
        "waterYear": waterYear,
        "datetime": dateList
    }

    siteListTable = "SELECT * FROM master_site"
    cursor.execute(siteListTable)
    result = cursor.fetchall()
    for line in result:

        siteID = line[3]
        nbsNum = line[2]
        citSciNum = line[4]

        nbsNum = nbsNum.split(".")[1]

        # generate the dataframe
        if siteID != "":
            hannaDict = getHanna(cursor, siteID)
            datetime = hannaDict["datetime"]
            index = hannaDict["index"]
            bp = hannaDict["barometricPressure_hanna"]
            keeperDict = {}
            keeperDict["datetime"] = datetime
            keeperDict["index"] = index
            keeperDict["barometricPressure_hanna"] = bp

            fullDict = joinDictSite(keeperDict, fullDict, siteID)

    fullDF = pd.DataFrame.from_dict(fullDict)
    return fullDF


def getSiteCoordinateDicts(cursor):
    siteListTable = "SELECT * FROM master_site"
    cursor.execute(siteListTable)
    result = cursor.fetchall()

    siteToX = {}
    siteToY = {}
    for line in result:

        siteID = line[3]

        if siteID != "":
            siteToX[siteID] = line[8]
            siteToY[siteID] = line[9]

    return siteToX, siteToY

def getDistance(x1, x2, y1, y2):
    x1 = float(x1)
    x2 = float(x2)
    y1 = float(y1)
    y2 = float(y2)

    dx = x1 - x2
    dy = y1 - y2

    hypsqr = dx**2 + dy**2
    dist = np.sqrt([hypsqr])
    dist = dist[0]
    return dist

def getStationToDistanceDict(xdict, ydict):
    stationToDistances = {}
    for station in xdict.keys():
        x1 = xdict[station]
        y1 = ydict[station]
        distances = []
        for stn in xdict.keys():
            x2 = xdict[stn]
            y2 = ydict[stn]

            distance = getDistance(x1, x2, y1, y2)
            distances.append(distance)
        stationToDistances[station] = distances

    return stationToDistances

def getClosestStationsDict(xdict, ydict):
    stationToClosest = {}
    stationToDistances = getStationToDistanceDict(xdict, ydict)
    stations = list(stationToDistances.keys())
    for station in stations:
        ds = stationToDistances[station]
        sortedDs = copy.copy(ds)
        sortedDs.sort()
        priorityList = []
        for distance in sortedDs:
            for i in range(len(stations)):
                if distance == stationToDistances[station][i]:
                    priorityList.append(stations[i])

        stationToClosest[station]  = priorityList

    return stationToClosest

def expandIndex(targetIndex, allIndices):
    newBools = []
    found_newBool = False
    for index in allIndices:
        if abs(index - targetIndex) < 0.1:
            newBools.append(True)
            found_newBool = True
        # if the first statement has been reached and passed, you can break the loop :)
        # elif found_newBool:
        #     break
        else:
            newBools.append(False)
    return newBools

# what is the right way to do this?

# calculate a barometric pressure column that I can subtract from the pressure measurements
# subract them
# run the standard curve
# calculate discharge

def getBarometricPressureColumn(siteID, pdf, stationToPriority):
    columnPostfix = "_barometricPressure_hanna"
    priorityList = stationToPriority[siteID]

    barometricData = pd.Series([None] * len(pdf[pdf.columns[0]]))
    barometricDataSites = pd.Series([None] * len(pdf[pdf.columns[0]]))

    mask = np.asarray(barometricData.isna())

    for site in priorityList:
        columnName = site + columnPostfix
        siteBarometricData = pdf[columnName]
        siteBarometricData = np.asarray(siteBarometricData)
        barometricData[mask] = siteBarometricData[mask]
        barometricDataSites = barometricDataSites.where(~mask, site)
        mask = np.asarray(barometricData.isna())
    barometricData = pd.Series(barometricData)
    barometricDataSites = barometricDataSites.where(~barometricData.isna(), None)

    bdf = pd.DataFrame({'data': barometricData, 'sites': barometricDataSites})
    bdf["corrections"] = [0] * len(bdf[bdf.columns[0]])
    bdf["corrected_values"] = [None] * len(bdf[bdf.columns[0]])

    # chooses how many steps to look forward/backward in determining average correction to apply
    interval = 2

    # finds unique batches and uses them to determine where to apply corrections
    batch_switches = bdf.drop_duplicates(subset='sites', keep='first')
    bdf["site_changed"] = bdf["sites"].shift(1, fill_value=bdf["sites"].head(1)) != bdf["sites"]
    bdf['site_changed'] = bdf['site_changed'].where(~bdf['data'].isna(), False)
    batch_switches = bdf[bdf['site_changed']]

    index_switches = batch_switches.index.tolist()
    cumulative_correction = 0

    for i in range(len(index_switches)):
        item = index_switches[i]
        prev_b = bdf["sites"][item - interval:item].tolist()
        next_b = bdf["sites"][item:item + interval].tolist()

        prev = bdf["data"][item - interval:item].tolist()
        next = bdf["data"][item:item + interval].tolist()

        if not all([elem is None for elem in prev_b]) and not all([elem is None for elem in next_b]):
            prev = [float(pressure) for pressure in prev if pressure is not None and pressure != ""]
            next = [float(pressure) for pressure in next if pressure is not None and pressure != ""]

            outliers = detect_outlier(prev)
            if len(outliers) > 0:
                print(f'help {outliers}')
                pass

            prev_no_outliers = list(set(prev) - set(outliers))
            avg_prev = mean(prev_no_outliers)

            outliers = detect_outlier(next)
            if len(outliers) > 0:
                print(f'help {outliers}')
                pass

            next_no_outliers = list(set(next) - set(outliers))
            avg_next = mean(next_no_outliers)

            correction_this_site = avg_prev - avg_next
            correction = correction_this_site + cumulative_correction

            if abs(correction) > 100:
                print(f'large correction: {correction_this_site}')

            mask = (bdf.index >= index_switches[i - 1]) & (bdf.index < index_switches[i])
            bdf['corrections'] = bdf['corrections'].where(~mask, correction)
            cumulative_correction = cumulative_correction + correction_this_site


    bdf['corrections'] = bdf['corrections'].where(~bdf['data'].isna(), None)
    bdf['corrected_values'] = pd.to_numeric(bdf['corrections']) + pd.to_numeric(bdf['data'])

    barometricData = bdf['corrected_values']

    return barometricData

def replaceBlankWithNone(array):
    array = list(array)
    for j in range(len(array)):
        val = array[j]
        val = str(val)
        val.replace(" ","")
        if val == "":
            array[j] = None
    return array

def getDischargeToPressureDF(df, siteID, pdf, stationToPriority, cursor):
    barometricData = getBarometricPressureColumn(siteID, pdf, stationToPriority)

    # step is equal to the difference in index equivalent to 3 hrs (12 indices == 12 15 min intervals == 3 hrs).
    step = 12

    # drop None values in pressure_hobo
    indices_no_na = pd.Series(barometricData[~barometricData.isna()].index)

    # create series of differences between indices so we can later find the jumps where Nones were taken out
    differences = indices_no_na.diff()
    # create series of indices of aforementioned differences ^
    index_df = differences.index
    # create dict and then df from series
    d = {'differences': differences, 'index_df': index_df}
    idf = pd.DataFrame(d)

    # starts == the beginning index of each individual grouping
    starts = idf.loc[(idf['differences'] >= step)]['index_df'].values.tolist()

    if len(starts) > 0:
        print("we have a huge err that I don't have the bandwidth to troubleshoot rn sorry :)")
        print("basically you have jumps within the barometric pressure data, idk maybe not a huge deal")

    # barometric_pairings = [df.loc[indices_no_na.values.tolist()[0]]['index'], df.loc[indices_no_na.values.tolist()[-1]]['index']]

    columnPostfix = "_barometricPressure_hanna"
    priorityList = stationToPriority[siteID]

    pdfMask = list(~df["discharge_measured"].isna())
    if len(pdfMask) != len(pdf):
        if len(pdfMask) < len(pdf):
            pdfMask.append(False)
        elif len(pdfMask) > len(pdf):
            pdfMask = pdfMask[:-1]

    try:
        dischargeIndices = pdf[pdfMask]["index"]
        
        # do for barometric pressure

        correctedPressurePoints = df[~df["discharge_measured"].isna()]["corrected_values"]
        correctionPoints = df[~df["discharge_measured"].isna()]["corrections"]
        pressurePoints = df[~df["discharge_measured"].isna()]["pressure_hobo"]
        pressureData = df["pressure_hobo"]
        correctedPressureData = df["corrected_values"]
        dischargePoints = df[~df["discharge_measured"].isna()]["discharge_measured"]
        datePoints = df[~df["discharge_measured"].isna()]["datetime"]

    except:
        print(traceback.format_exc())
        print("tears")

    xs = []
    ys = []
    zs = []
    dates = []

    press = []
    corrected_press = []
    corrections_short = []
    corrections_full = []
    barPress = []
    dis = []
    fullDates = []
    for i in range(len(dischargeIndices)):
        index = list(dischargeIndices)[i]
        pressure = list(pressurePoints)[i]
        discharge = list(dischargePoints)[i]
        date = list(datePoints)[i]
        corrected_pressure = list(correctedPressurePoints)[i]
        correction = list(correctionPoints)[i]

        expandedIndex = expandIndex(index, df["index"])
        expandedIndex = expandedIndex[:len(pdf[pdf.columns[0]])]

        # because "" values can be in there instead of None values
        nearbyBarometricMeasurements = barometricData[expandedIndex]
        nearbyPressureMeasurements = pressureData[:len(barometricData)][expandedIndex]
        nearbyCorrectedPressureMeasurements = correctedPressureData[:len(barometricData)][expandedIndex]

        nearbyBarometricMeasurements = replaceBlankWithNone(nearbyBarometricMeasurements)
        nearbyPressureMeasurements = replaceBlankWithNone(nearbyPressureMeasurements)
        nearbyCorrectedPressureMeasurements = replaceBlankWithNone(nearbyCorrectedPressureMeasurements)

        nearbyBarometricMeasurements = pd.Series(nearbyBarometricMeasurements)
        nearbyPressureMeasurements = pd.Series(nearbyPressureMeasurements)
        nearbyCorrectedPressureMeasurements = pd.Series(nearbyCorrectedPressureMeasurements)

        maskB = np.asarray(~nearbyBarometricMeasurements.isna())
        maskP = np.asarray(~nearbyPressureMeasurements.isna())
        maskC = np.asarray(~nearbyCorrectedPressureMeasurements.isna())

        nearbyBarometricMeasurements = np.asarray(nearbyBarometricMeasurements)
        nearbyPressureMeasurements = np.asarray(nearbyPressureMeasurements)
        nearbyCorrectedPressureMeasurements = np.asarray(nearbyCorrectedPressureMeasurements)

        if np.sum(maskB) > 0:
            meanBarometricPressure = np.mean(nearbyBarometricMeasurements[maskB])
        else:
            meanBarometricPressure = None

        if np.sum(maskP) > 0:
            meanPressure = np.mean(nearbyPressureMeasurements[maskP])
        else:
            meanPressure = None

        if np.sum(maskC) > 0:
            meanCorrectedPressure = np.mean(nearbyCorrectedPressureMeasurements[maskC])
        else:
            meanCorrectedPressure = None

        press.append(meanPressure)
        corrected_press.append(meanCorrectedPressure)
        barPress.append(meanBarometricPressure)
        dis.append(discharge)
        fullDates.append(date)
        corrections_full.append(correction)

        if meanBarometricPressure != None and meanCorrectedPressure != None and discharge != None:
            if not np.isnan(meanBarometricPressure) and not np.isnan(meanPressure) and not np.isnan(discharge):
                discounted_pressure_point = float(meanPressure) - float(meanBarometricPressure)
                if discounted_pressure_point > -15:
                    xs.append(discounted_pressure_point)
                    zs.append(float(meanCorrectedPressure) - float(meanBarometricPressure))
                    ys.append(discharge)
                    dates.append(date)
                    corrections_short.append(correction)
                else:
                    print('err')

    returnDict = {"barometric_discounted_original_pressure": xs, "barometric_discounted_corrected_pressure": zs, "measured_discharge":ys, "datetime":dates, "corrections": corrections_short}
    longDict = {"barometricPressure": barPress, "absolutePressure": press,"discharge": dis, "datetime": fullDates, "correctedPressure": corrected_press, "corrections": corrections_full}
    # FIXME: this isn't quite getting it right!
    returnDF = pd.DataFrame.from_dict(returnDict)
    longDF = pd.DataFrame.from_dict(longDict)
    return returnDF, longDF

def interpolate(df):
    print("interpolation not functional yet")
    return df

def detect_outlier(data_1):
    outliers = []
    ##### change threshold? Ask Brian
    threshold = 2.5
    mean_1 = np.mean(data_1)
    std_1 = np.std(data_1)

    for y in data_1:
        z_score = (y - mean_1) / std_1
        if np.abs(z_score) > threshold and z_score < 0:
            outliers.append(y)
    return outliers

def processDFStandardCurve(cursor, siteID, nbsNum, citSciNum, testsDict, optionsDict, outputPath, pdf, stationToPriority):
    if optionsDict["calculateStandardCurve"] == True:
        testsDict["hoboPressure"] = True
        testsDict["measuredDischarge"] = True

    ### update testsDict (options?) to grab batch # from database, when batch numbers switch,
    df = makeSiteDF(cursor, siteID, nbsNum, citSciNum, testsDict, optionsDict)
    df["corrections"] = [0] * len(df[df.columns[0]])
    df["corrected_values"] = [None] * len(df[df.columns[0]])

    if optionsDict["include_batch_id"]:
        # chooses how many steps to look forward/backward in determining average correction to apply
        interval = 12

        # finds unique batches and uses them to determine where to apply corrections
        batch_switches = df.drop_duplicates(subset='batch_id', keep='first')
        index_switches = batch_switches.index.tolist()

        for i in range(len(index_switches)):
            item = index_switches[i]
            prev_b = df["batch_id"][item - interval:item].tolist()
            next_b = df["batch_id"][item:item + interval].tolist()

            prev = df["pressure_hobo"][item - interval:item].tolist()
            next = df["pressure_hobo"][item:item + interval].tolist()

            if not all([elem is None for elem in prev_b]) and not all([elem is None for elem in next_b]):
                prev = [float(pressure) for pressure in prev if pressure is not None and pressure != ""]
                next = [float(pressure) for pressure in next if pressure is not None and pressure != ""]

                outliers = detect_outlier(prev)
                if len(outliers) > 0:
                    print(f'help {outliers}')
                    pass

                prev_no_outliers = list(set(prev) - set(outliers))
                avg_prev = mean(prev_no_outliers)

                outliers = detect_outlier(next)
                if len(outliers) > 0:
                    print(f'help {outliers}')
                    pass

                next_no_outliers = list(set(next) - set(outliers))
                avg_next = mean(next_no_outliers)

                correction = avg_prev - avg_next
                if abs(correction) > 100:
                    print(f'large correction: {correction}')

                mask = (index_switches[i - 1] <= df.index) & (df.index > index_switches[i])
                df['corrections'] = df['corrections'].where(~mask, correction)

    df['corrections'] = df['corrections'].where(~df['pressure_hobo'].isna(), None)
    df['corrected_values'] = pd.to_numeric(df['corrections']) + pd.to_numeric(df['pressure_hobo'])

    # -------------------------------------
    # Separate DF into continuous segments
    # -------------------------------------

    # step is equal to the difference in index equivalent to 3 hrs (12 indices == 12 15 min intervals == 3 hrs).
    step = df['index'].diff().mean() * 12

    # drop None values in pressure_hobo
    indices_no_na = df[~df['pressure_hobo'].isna()]['index']
    indices_no_na = indices_no_na.reset_index()
    indices_no_na = indices_no_na['index']

    # create series of differences between indices so we can later find the jumps where Nones were taken out
    differences = indices_no_na.diff()
    # create series of indices of aforementioned differences ^
    index_df = differences.index
    # create dict and then df from series
    d = {'differences': differences, 'index_df': index_df}
    idf = pd.DataFrame(d)
    idf = idf.reset_index()

    # starts == the beginning index of each individual grouping
    starts = idf.loc[(idf['differences'] >= step)]['index_df'].values.tolist()

    # ends == the ending index of each individual grouping
    ends = [i - 1 for i in starts]

    # add beginning and closing index to starts
    starts.insert(0, indices_no_na.index.tolist()[0])
    starts.append(indices_no_na.index.tolist()[-1])

    final = sorted(ends + starts)

    # use indices of indices_no_na to find corresponding target df indices
    final = [indices_no_na.loc[i] for i in final]
    len_final = len(final)
    if len_final % 2 != 0:
        print('it should really be even, mayday!! 😰')

    # pair those indices and you're done :)
    pairings = [[final[i], final[i+1]] for i in range(0, len(final), 2)]


    # This line is obnoxiously obtuse but I don't have the bandwidth to simplify rn. Basically it takes the column ['index'] and uses it to find the actual indices from df
    list_df = [df[df.loc[df['index'] == pair[0]].index.tolist()[0]:df.loc[df['index'] == pair[1]].index.tolist()[0]] for pair in pairings]

    for i in range(len(list_df)):
        df1, df2 = getDischargeToPressureDF(list_df[i], siteID, pdf, stationToPriority, cursor)

        plotRatingCurve(df1, pairings[i])

        df1.to_csv(f"{outputPath}/pressure_to_discharge_no_null_{pairings[i][0]}_to_{pairings[i][1]}_{str(siteID)}.csv")
        df2.to_csv(f"{outputPath}/pressure_and_barometric_full_{pairings[i][0]}_to_{pairings[i][1]}_{str(siteID)}.csv")

    # download the df
    # download the picture
    # generate the figure
    # download the figure
    # download the csv # date, pressure, discharge,
    # save the slopes onto the database

def plotRatingCurve(df, pair):
    res = stats.linregress(df["barometric_discounted_original_pressure"], df["measured_discharge"])

    plt.figure(figsize=(17, 7))
    plt.style.use('ggplot')
    plt.xlabel("barometric discounted pressure")
    plt.ylabel("discharge")

    plt.scatter(df["barometric_discounted_original_pressure"], df["measured_discharge"], s=6, c="tomato", zorder=4, label=f"discharge")
    plt.plot(df["barometric_discounted_original_pressure"], res.intercept + res.slope * df["barometric_discounted_original_pressure"], 'r', c='teal', label='fitted line')
    plt.title("rating curve")
    plt.legend()
    plt.savefig(f"{outputPath}/{siteID}_{pair[0]}_to_{pair[1]}_rating_curve.png", dpi=300)
    plt.clf()
    plt.close()

def getSlopeIntercept(datetime, siteID, keyDict, siteDict):
    date, time = datetimes.split(" ")
    year, month, day = date.split("/")
    for key in siteDict.keys():
        vec = siteDict[key]
        slope = vec[keyDict["slope"]]
        intercept = vec[keyDict["intercept"]]
        startIndex = vec[keyDict["startIndex"]]
        endIndex = vec[keyDict["endIndex"]]

        # if
        #     return slope, intercept # FIXME not sure wher eintercept is coming from

    print("WARNING getSlopeIntercept not implemented")
    return 1, 1


def addCalculatedDischarge(df, siteID, pdf, stationToPriority, cursor):
    barometricData = getBarometricPressureColumn(siteID, pdf, stationToPriority)
    absoluteData = df["pressure_hobo"]
    dates = df["datetime"]
    print(len(barometricData))
    print(len(absoluteData))

    mask1 = np.asarray(~barometricData.isna())
    mask2 = np.asarray(~absoluteData.isna())
    mask = np.logical_and(mask1, mask2)

    barometricData = np.asarray(barometricData)
    absoluteData = np.asarray(absoluteData)
    pressureData = absoluteData

    pressureData[mask] = float(absoluteData[mask]) - float(barometricData[mask])
    pressureData[~mask] = None
    # print(pressureData)
    # print(pressureData[mask])

    dischargePoints = [None] * len(pressureData)

    keyDict, siteDict = getSlopeInterceptDicts(cursor, siteID)

    for i in range(len(pressureData[mask])):
        date = dates[mask][i]
        slope, intercept = getSlopeIntercept(date, siteID, cursor, keyDict, siteDict)

        pressure = pressureData[mask][i]
        discharge = pressure * slope + intercept
        dischargePoints[mask][i] = discharge

    df["calculated_discharge"] = dischargePoints

    return df

def processDF(cursor, siteID, nbsNum, citSciNum, testsDict, optionsDict, outputPath, pdf, stationToPriority):

    if optionsDict["calculateDischarge"] == True:
        testsDict["hoboPressure"] = True

    df = makeSiteDF(cursor, siteID, nbsNum, citSciNum, testsDict, optionsDict)

    # calculate discharge 
    if optionsDict["calculateDischarge"] == True:
        df = addCalculatedDischarge(df, siteID, pdf, stationToPriority, cursor)

    # interpolate
    if optionsDict["interpolate"] == True:
        df = interpolate(df)

    if optionsDict["correct_values"] == True:
        sensors = ["Hanna", "YSI"]
        target_list = ["electricalConductivity", "pH", "temperature", "orpMV", "dissolvedOxygen_mgL"]
        corrections_df, device = correctValuesCurve(df, sensors, target_list)

        if corrections_df is None:
            if siteID == "":
                print(
                    f"huge error! corrections for NBS{nbsNum} failed because no hobo/eureka/hanna data was found. Possibly it only has scan data?")
            else:
                print(
                    f"Huge error! Corrections for {siteID} failed because no hobo/eureka/hanna data was found. Possibly it only has scan data?")
            saveDF(df, outputPath, siteID, nbsNum)
        else:
            saveDF(df, outputPath, siteID, nbsNum, True, corrections_df, target_list, sensors, device)
    else:
        saveDF(df, outputPath, siteID, nbsNum)

def objective(x, a, b, c, d, e, f, g, h):
    return (a * x) + (b * x ** 2) + (c * x ** 3) + (d * x ** 4) + (e * x ** 5) + (f * x ** 6) + (g * x ** 7) + h

def correctValuesCurve(df, sensors, target_list):
    df = df.replace("", np.nan, regex=True)
    df = df.fillna(value=np.nan)

    device = senseDeviceType(target_list, df)
    if device is None:
        return None, None

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
        return None, None

    return df, device # FIXME

    for target in target_list:
        filtered_df = df[~df[f"{target}_fieldsheet"].isna()]
        x = filtered_df["index"].tolist()
        y = filtered_df[f"{target}_fieldsheet"].tolist()
        minind = x[0]
        maxind = x[-1]

        if len(x) < 3:
            continue

        popt, _ = curve_fit(objective, x, y)

        df[f"YSI_curve_{target}"] = objective(df["index"], *popt)

        # cut off errant tails of curve
        df[f"YSI_curve_{target}"] = df[f"YSI_curve_{target}"].where((df["index"] > minind), None)
        df[f"YSI_curve_{target}"] = df[f"YSI_curve_{target}"].where((df["index"] < maxind), None)

        # filter df for creating residuals between ysi curve and data points, find residual points
        filtered_df = df[~df[f"{target}_fieldsheet"].isna()]
        filtered_df = filtered_df[~filtered_df[f"{target}_{device}"].isna()]
        filtered_df = filtered_df[~filtered_df[f"YSI_curve_{target}"].isna()]

        filtered_df[f"residual_curve_{target}"] = [None] * len(filtered_df)
        # fdf["res_curve"] = fdf["YSI_curve"] - fdf["electricalConductivity_eureka"]
        filtered_df[f"residual_curve_{target}"] = filtered_df[f"YSI_curve_{target}"] - filtered_df[f"{target}_{device}"]
        asdf = copy.copy(filtered_df)
        asdf = asdf.rename(columns={f"residual_curve_{target}": f"{target}"})
        asdf = asdf[f"{target}"]
        df = pd.concat([df, asdf], axis=1)

        res = filtered_df[f"residual_curve_{target}"].tolist()
        x = filtered_df["index"].tolist()

        # add residual points to df
        filtered_df = filtered_df[f"residual_curve_{target}"]
        df = pd.concat([df, filtered_df], axis=1)

        # optimize curve for residual points
        popt, _ = curve_fit(objective, x, res)

        filtered_df = df[~df[f"residual_curve_{target}"].isna()]
        x = filtered_df["index"].tolist()
        minind = x[0]
        maxind = x[-1]

        df[f"residual_curve_{target}"] = objective(df["index"], *popt)
        df[f"residual_curve_{target}"] = df[f"residual_curve_{target}"].where((df["index"] > minind), None)
        df[f"residual_curve_{target}"] = df[f"residual_curve_{target}"].where((df["index"] < maxind), None)

        df[f"{target}_{device}_corrected"] = df[f"{target}_{device}"] + df[f"residual_curve_{target}"]

    return df, device

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

def senseDeviceType(target_list, df):
    parse_df = df[~df[f"{target_list[0]}_fieldsheet"].isna()]
    if not all(parse_df["conductivity_hobo"].isna()):
        device = "hobo"
    elif not all(parse_df["electricalConductivity_eureka"].isna()):
        device = "eureka"
    elif "electricalConductivity_hanna" in parse_df.columns.tolist() and not all(parse_df["electricalConductivity_hanna"].isna()):
        device = "hanna"
    else:
        device = ""
        print("HUGE ERR: no device detected :(")
        print("see whether you can trace the error?")
    return device

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

def downloadTimeSeries(outputPath, testsDict, optionsDict, cursor):
    print("SYSTEM: ", platform.system())

    if optionsDict["calculateDischarge"] == True:
        pdf = getAllHannaPressuresDF(cursor)
        xdict, ydict = getSiteCoordinateDicts(cursor)
        stationToPriority = getClosestStationsDict(xdict, ydict)
    else:
        pdf = None
        stationToPriority = None
    if not os.path.isdir(outputPath + "/timeSeriesCorrectionFigures"):
        os.makedirs(outputPath + "/timeSeriesCorrectionFigures")
    siteListTable = "SELECT * FROM master_site"
    cursor.execute(siteListTable)
    result = cursor.fetchall()

    progress_length = len(result)
    progress_list = [x[3] for x in result]
    
    for line in result:
        print(f"Now starting {line[3] if line[3] != '' else str(line[2])}")
        print("[" + (("#") * (progress_length - len(progress_list))) + ((" ") * len(progress_list)) + "]")
        print(str(round(((((progress_length - len(progress_list)) / (progress_length)) * 100)), 2)) + "% done" + "\n")

        siteID = line[3]
        nbsNum = line[2]
        citSciNum = line[4]

        nbsNum = nbsNum.split(".")[1]

        # generate the dataframe
        if optionsDict["includeSynoptic"] == True:
            df = processDF(cursor, siteID, nbsNum, citSciNum, testsDict, optionsDict, outputPath, pdf, stationToPriority)
        else:
            if siteID != "":
                df = processDF(cursor, siteID, nbsNum, citSciNum, testsDict, optionsDict, outputPath, pdf, stationToPriority)
        progress_list.remove(siteID)
        print(f"{line[3] if line[3] != '' else str(nbsNum)} complete")

    return "successfully downloaded time series report to " + outputPath

def downloadStandardCurve(outputPath, testsDict, optionsDict, cursor):

    for test in testsDict.keys():
        testsDict[test] = False
    for option in optionsDict.keys():
        optionsDict[option] = False

    # double check that the right things are being requested
    testsDict["pressure_hobo"] = True
    optionsDict["include_batch_id"] = True
    optionsDict["calculateStandardCurve"] = True  # FIXME: figuring out what to do with calculateDischarge

    pdf = getAllHannaPressuresDF(cursor)
    xdict, ydict = getSiteCoordinateDicts(cursor)
    stationToPriority = getClosestStationsDict(xdict, ydict)

    siteListTable = "SELECT * FROM master_site"
    cursor.execute(siteListTable)
    result = cursor.fetchall()

    progress_length = len(result)
    progress_list = [x[3] for x in result]

    for line in result:
        print(f"Now starting {line[3] if line[3] != '' else str(line[2])}")
        print("[" + (("#") * (progress_length - len(progress_list))) + ((" ") * len(progress_list)) + "]")
        print(str(round(((((progress_length - len(progress_list)) / (progress_length)) * 100)), 2)) + "% done" + "\n")

        siteID = line[3]
        nbsNum = line[2]
        citSciNum = line[4]

        nbsNum = nbsNum.split(".")[1]

        if siteID != "":
            processDFStandardCurve(cursor, siteID, nbsNum, citSciNum, testsDict, optionsDict, outputPath, pdf, stationToPriority)

    return "successfully downloaded standard curve report to " + outputPath

def downloadLoggerGapsReport(outputPath, cursor, testsDict, optionsDict):
    siteListTable = "SELECT * FROM master_site"
    cursor.execute(siteListTable)
    result = cursor.fetchall()

    for line in result:

        siteID = line[3]
        nbsNum = line[2]
        citSciNum = line[4]

        nbsNum = nbsNum.split(".")[1]

        if siteID != "":
            dataDict = {} 
            
            df = makeSiteDF(cursor, siteID, nbsNum, citSciNum, testsDict, optionsDict)
            for column in df:
                if column != "index" and column != "datetime":
                    dataDict[column] = []
                    
                    startDates = []
                    stopDates = []
                    
                    values = df[column]
                    dates = df["datetime"]
                    indices = df["index"]
                    
                    keeperMask = ~values.isna()
                    
                    values = np.asarray(values)
                    dates = np.asarray(dates)
                    indices = np.asarray(indices)
                    
                    keeperValues = values[keeperMask]
                    keeperDates = dates[keeperMask]
                    keeperIndices = indices[keeperMask]

                    if len(keeperIndices) > 0:
                        firstRecordedDate = keeperDates[0]
                        lastRecordedDate = keeperDates[-1]

                    numPossibleDays = len(keeperIndices) / (4 * 24)
                     
                    for i in range(1, len(keeperIndices)):
                        previous = keeperIndices[i - 1]
                        current = keeperIndices[i]
                         
                        if current - previous > 5: # if we go for ~ more than an hour without data
                            stopDate = keeperDates[i - 1]    
                            startDate = keeperDates[i]
                            
                            stopDates.append(stopDate)
                            startDates.append(startDate)


                    # find the missing values
                    # grab the dates associated with the start and finish
                    # 

                    startToStopDates = dict(zip(startDates, stopDates))

                    if len(keeperIndices) == 0:
                        dataDict[column].append("no data logged")
                    else:
                        dataDict[column].append("first logged date")
                        dataDict[column].append(firstRecordedDate)
                        for key in startToStopDates.keys():
                            dataDict[column].append("start of gap")
                            dataDict[column].append(startToStopDates[key])
                            dataDict[column].append("end of gap")
                            dataDict[column].append(key)
                        dataDict[column].append("last logged date")
                        dataDict[column].append(lastRecordedDate)

        # to csv via a dataframe
        maxLenList = 0
        for key in dataDict.keys():
            if len(dataDict[key]) > maxLenList:
                maxLenList = len(dataDict[key])
        for key in dataDict.keys():
            extra = [None] * (maxLenList - len(dataDict[key]))
            dataDict[key] = dataDict[key] + extra

        missingDf = pd.DataFrame.from_dict(dataDict)
        if platform.system() == "Windows":
            filePath = outputPath + "\\loggerGapsReport_" + siteID + ".csv"
        else:
            filePath = outputPath = "/loggerGapsReport_" + siteID + ".csv"
        missingDf.to_csv(filePath, index=False)

    return "successfully downloaded logger gaps report to " + outputPath
