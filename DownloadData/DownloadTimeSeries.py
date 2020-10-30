import sqlite3
from DownloadData.SQLQueries import *
import pandas as pd
from DownloadData.DateToIndex import *
import numpy as np
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression

def getIndexList():
    # go from the start date to now
    now = str(datetime.now())
    date, time = now.split(" ")
    year, month, day = date.split("-")
    year = year[:2]
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
                listIndex = int(index / dayToIndexRatio) - int(startIndex / dayToIndexRatio)

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

            for i in range(len(indices)):
                index = indices[i]
                listIndex = int(index / dayToIndexRatio) - int(startIndex / dayToIndexRatio)

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

    if testsDict["fieldSheetInfo"]:
        try:
            fieldSheetDict = getFieldSheetInfo(cursor, siteID, nbsNum, citSciNum)
            fullDict = joinDict(fieldSheetDict, fullDict)
        except:
            print("ERROR")
    if testsDict["hoboPressure"] or optionsDict["calculateDischarge"]:
        try:
            pDict = getP(cursor, siteID)
            fullDict = joinDict(pDict, fullDict)
        except:
            print("ERROR")
    if testsDict["hoboLight"]:
        try:
            lightDict = getLightHobo(cursor, siteID)
            fullDict = joinDict(lightDict, fullDict)
        except:
            print("error")
    if testsDict["hoboConductivity"]:
        try:
            condDict = getConductivityHobo(cursor, siteID)
            fullDict = joinDict(condDict, fullDict)
        except:
            print("error")
    if testsDict["hoboOxygen"]:
        try:
            oxygenDict = getOxygenHobo(cursor, siteID)
            fullDict = joinDict(oxygenDict, fullDict)
        except:
            print("error")
    if testsDict["measuredDischarge"]:
        try:
            qDict = getQ(cursor, siteID)
            fullDict = joinDict(qDict, fullDict)
        except:
            print("error")
    if testsDict["hanna"]:
        try:
            hannaDict = getHanna(cursor, siteID)
            fullDict = joinDict(hannaDict, fullDict)
        except:
            print("error")
    if testsDict["eureka"]:
        try:
            eurekaDict = getEureka(cursor, siteID)
            fullDict = joinDict(eurekaDict, fullDict)
        except:
            print("error")
    if testsDict["elementar"]:
        try:
            elementarDict = getElementar(cursor, siteID, nbsNum, citSciNum)
            fullDict = joinDict(elementarDict, fullDict)
        except:
            print("error")
    if testsDict["scanCalculated"]:
        try:
            scanParDict = getScanPar(cursor, siteID, nbsNum, citSciNum)
            fullDict = joinDict(scanParDict, fullDict)
        except:
            print("error")
    if testsDict["scanRaw"]:
        try:
            scanFPDict = getScanFp(cursor, siteID, nbsNum, citSciNum)
            fullDict = joinDict(scanParDict, fullDict)
        except:
            print("error")
    if testsDict["ic"]:
        try:
            icCationDict = getICCation(cursor, siteID, nbsNum, citSciNum)
            fullDict = joinDict(icCationDict, fullDict)
        except:
            print("error")
    if testsDict["ic"]:
        try:
            icAnionDict = getICAnion(cursor, siteID, nbsNum, citSciNum)
            fullDict = joinDict(icAnionDict, fullDict)
        except:
            print("error")
    if testsDict["icp"]:
        try:
            icpDict = getICP(cursor, siteID, nbsNum, citSciNum)
            fullDict = joinDict(icpDict, fullDict)
        except:
            print("error")
        
    print(siteID + " complete")
    df = pd.DataFrame.from_dict(fullDict)
    return df

def saveDF(df, outputPath, siteID, nbsNum):
    if siteID != "":
        filePath = outputPath + "\\timeSeriesReport_" + siteID + ".csv"
    else:
        filePath = outputPath + "\\timeSeriesReport_NBS" + nbsNum + ".csv"
    print(filePath)
    df.to_csv(filePath, index=False)


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
        sortedDs = ds
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
    for index in allIndices:
        if abs(index - targetIndex) < 0.1:
            newBools.append(True)
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
    mask = np.asarray(barometricData.isna())

    for site in priorityList:
        columnName = site + columnPostfix
        siteBarometricData = pdf[columnName]
        siteBarometricData = np.asarray(siteBarometricData)
        barometricData[mask] = siteBarometricData[mask]
        mask = np.asarray(barometricData.isna())
    barometricData = pd.Series(barometricData)
    return barometricData

def getDischargeToPressureDF(df, siteID, pdf, stationToPriority, cursor):
    barometricData = getBarometricPressureColumn(siteID, pdf, stationToPriority)

    columnPostfix = "_barometricPressure_hanna"
    priorityList = stationToPriority[siteID]

    print(df)
    dischargeIndices = pdf[~df["discharge_measured"].isna()]["index"]
    pressurePoints = df[~df["discharge_measured"].isna()]["pressure_hobo"]
    pressureData = df["pressure_hobo"]
    dischargePoints = df[~df["discharge_measured"].isna()]["discharge_measured"]
    datePoints = df[~df["discharge_measured"].isna()]["datetime"]

    xs = []
    ys = []
    dates = []

    press = []
    barPress = []
    dis = []
    fullDates = []
    for i in range(len(dischargeIndices)):
        index = list(dischargeIndices)[i]
        pressure = list(pressurePoints)[i]
        discharge = list(dischargePoints)[i]
        date = list(datePoints)[i]

        expandedIndex = expandIndex(index, df["index"])
        expandedIndex = expandedIndex[:len(pdf[pdf.columns[0]])]
        nearbyBarometricMeasurements = pd.Series(barometricData[expandedIndex])
        nearbyPressureMeasurements = pd.Series(pressureData[:len(barometricData)][expandedIndex])

        maskB = np.asarray(~nearbyBarometricMeasurements.isna())
        maskP = np.asarray(~nearbyPressureMeasurements.isna())

        nearbyBarometricMeasurements = np.asarray(nearbyBarometricMeasurements)
        nearbyPressureMeasurements = np.asarray(nearbyPressureMeasurements)

        if np.sum(maskB) > 0:
            meanBarometricPressure = np.mean(nearbyBarometricMeasurements[maskB])
        else:
            meanBarometricPressure = None

        if np.sum(maskP) > 0:
            meanPressure = np.mean(nearbyPressureMeasurements[maskP])
        else:
            meanPressure = None

        press.append(meanPressure)
        barPress.append(meanBarometricPressure)
        dis.append(discharge)
        fullDates.append(date)

        if meanBarometricPressure != None and meanPressure != None and discharge != None:
            if not np.isnan(meanBarometricPressure) and not np.isnan(meanPressure) and not np.isnan(discharge):
                xs.append(float(meanPressure) - float(meanBarometricPressure))
                ys.append(discharge)
                dates.append(date)

    returnDict = {"barometric_discounted_pressure":xs,
                      "measured_discharge":ys, "datetime":dates}
    longDict = {"barometricPressure":barPress,"absolutePressure":press,"discharge":dis, "datetime":fullDates}
    returnDF = pd.DataFrame.from_dict(returnDict)
    longDF = pd.DataFrame.from_dict(longDict)
    return returnDF, longDF


def interpolate(df):
    print("interpolation not functional yet")
    return df


def processDFStandardCurve(cursor, siteID, nbsNum, citSciNum, testsDict, optionsDict, outputPath, pdf, stationToPriority):
    if optionsDict["calculateStandardCurve"] == True:
        testsDict["hoboPressure"] = True
        testsDict["measuredDischarge"] = True

    df = makeSiteDF(cursor, siteID, nbsNum, citSciNum, testsDict, optionsDict)

    # calculate discharge

    df1, df2 = getDischargeToPressureDF(df, siteID, pdf, stationToPriority, cursor)

    df1.to_csv(outputPath + "/pressure_to_discharge_no_null_" + str(siteID) + ".csv")
    df2.to_csv(outputPath + "/pressure_and_barometric_full_" + str(siteID) + ".csv")

    # download the df
    # download the picture
    # generate the figure
    # dowload the figure
    # download the csv # date, pressure, discharge,
    # save the slopes onto the database



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

    pressureData[mask] = absoluteData[mask] - barometricData[mask]
    pressureData[~mask] = None
    print(pressureData)
    print(pressureData[mask])

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

        print(df["calculated_discharge"])
        pass


    # interpolate
    if optionsDict["interpolate"] == True:
        df = interpolate(df)

    saveDF(df, outputPath, siteID, nbsNum)

def downloadTimeSeries(outputPath, testsDict, optionsDict, cursor):
    if optionsDict["calculateDischarge"] == True:
        pdf = getAllHannaPressuresDF(cursor)
        xdict, ydict = getSiteCoordinateDicts(cursor)
        stationToPriority = getClosestStationsDict(xdict, ydict)
    else:
        pdf = None
        stationToPriority = None

    siteListTable = "SELECT * FROM master_site"
    cursor.execute(siteListTable)
    result = cursor.fetchall()
    for line in result:

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

    return "successfully downloaded time series report to " + outputPath





def downloadStandardCurve(outputPath, testsDict, optionsDict, cursor):

    for test in testsDict.keys():
        testsDict[test] = False
    for option in optionsDict.keys():
        optionsDict[option] = False

    # double check that the right things are being requested
    testsDict["pressure_hobo"] = True
    optionsDict["calculateStandardCurve"] = True  # FIXME: figuring out what to do with calculateDischarge

    pdf = getAllHannaPressuresDF(cursor)
    xdict, ydict = getSiteCoordinateDicts(cursor)
    stationToPriority = getClosestStationsDict(xdict, ydict)

    siteListTable = "SELECT * FROM master_site"
    cursor.execute(siteListTable)
    result = cursor.fetchall()
    for line in result: 

        siteID = line[3]
        nbsNum = line[2]
        citSciNum = line[4]

        nbsNum = nbsNum.split(".")[1]

        print("new site ***************** ")
        print(siteID)

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

        filePath = outputPath + "\\loggerGapsReport_" + siteID + ".csv"
        missingDf.to_csv(filePath, index=False)

    return "successfully downloaded logger gaps report to " + outputPath
