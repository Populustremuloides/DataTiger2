from datetime import datetime
import pandas as pd
from DownloadData.DateToIndex import *
from DownloadData.SQLQueries import *
from USGS_downloaders.scrape_usgs_catchments import *

#----------------------------------------------------------------------#
#---------------------MAKE STANDARD CURVES SITE DF---------------------#
#----------------------------------------------------------------------#

###Adds all tests onto *dischargeDfDict* with cursors, then creates sites *df*###
def makeStandardCurveSiteDF(cursor, siteID, testsDict, optionsDict):
    indexList = getIndexList()
    dateList = getDateList(indexList)
    indexInWaterYear, waterYear = getIndexInWaterYearList(dateList, indexList)

    dischargeDfDict = {
        "index": indexList,
        "indexInWaterYear": indexInWaterYear,
        "waterYear": waterYear,
        "datetime": dateList
    }

    if testsDict["hoboPressure"] or optionsDict["calculateDischarge"]:
        # try:
        pDict = getP(cursor, siteID)
        dischargeDfDict = joinDict(pDict, dischargeDfDict)
    # except:
    #     print("ERROR")
    if testsDict["measuredDischarge"]:
        # try:
        qDict = getQ(cursor, siteID)
        dischargeDfDict = joinDict(qDict, dischargeDfDict)
    # except:
    #     print("error")

    dischargeDf = pd.DataFrame.from_dict(dischargeDfDict)
    if optionsDict["include_batch_id"]:
        pass
    elif "batch_id" in dischargeDf.columns:
        dischargeDf = dischargeDf.drop("batch_id", axis=1)
        ###
    return dischargeDf

###Joins cursor data to *dischargeDfDict* in order to make site df###
def joinDict(dict1, dischargeDfDict):
    # Joins all of the cursor data to the ongoing dataframe sent in with it.
    dataNames = dict1.keys()
    for name in dataNames:
        if name == "index" or name == "datetime":
            pass

        elif name == "batch_id" and "batch_id" in dischargeDfDict.keys() and len(dischargeDfDict["batch_id"]) == len(
                dischargeDfDict["index"]):
            pass

        else:
            dataList = dict1[name]
            indices = dict1["index"]
            dtime = dict1["datetime"]
            newData = [None] * len(dischargeDfDict["index"])
            # print(fullDict["datetime"][-5:])
            for i in range(len(indices)):
                index = indices[i]
                # listIndex = int(index / dayToIndexRatio) - int(startIndex / dayToIndexRatio)
                listIndex = round(index * indexToDayRatio)

                data = dataList[i]
                dt = dtime[i]
                newData[listIndex] = data

            dischargeDfDict[name] = newData
    return dischargeDfDict

#-----------------------------------------------------------------------#
#------------------------MAKE HANNA PRESSURES DF------------------------#
#-----------------------------------------------------------------------#

###Creates *pdf* through cursor that pulls all barometric pressure readings from Hanna devices###
def getAllHannaPressuresDF(cursor):
    indexList = getIndexList()
    dateList = getDateList(indexList)
    indexInWaterYear, waterYear = getIndexInWaterYearList(dateList, indexList)

    hannaPressuresDfDict = {
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
            keeperDict = {}
            hannaDict = getHanna(cursor, siteID)
            keeperDict["datetime"] = hannaDict["datetime"]
            keeperDict["index"] = hannaDict["index"]
            keeperDict["barometricPressure_hanna"] = hannaDict["barometricPressure_hanna"]

            hannaPressuresDfDict = joinDictSite(keeperDict, hannaPressuresDfDict, siteID)

    pdf = pd.DataFrame.from_dict(hannaPressuresDfDict)
    return pdf

###Joins *index*, *datetime*, site *bp* to create *hannaPressuresDfDict*###
###site barometric pressure is left as a vector of 'NONE', until added later###
def joinDictSite(dict1, hannaPressuresDfDict, siteID):
    # All in all, it adds the sites index, datetime, and barometric pressure values to the dataframe
    dataNames = dict1.keys()
    for name in dataNames:
        if name == "index" or name == "datetime":
            pass
        else:

            dataList = dict1[name]
            indices = dict1["index"]

            # Creates vector the length of the index with values of "None" which will be replaced with barometric pressure reads
            newData = [None] * len(hannaPressuresDfDict["index"])

            for i in range(len(indices)):
                index = indices[i]
                # listIndex = int(index / dayToIndexRatio) - int(startIndex / dayToIndexRatio)
                listIndex = round(index / dayToIndexRatio)

                data = dataList[i]

                newData[listIndex] = data

            newName = siteID + "_" + name
            hannaPressuresDfDict[newName] = newData
    return hannaPressuresDfDict

#-----------------------------------------------------------------------#
#--------------------------MULTI USE FUNCTIONS--------------------------#
#-----------------------------------------------------------------------#

###Creates value using today's date and giving it a numerical value based on the start date of the project###
###Creates *indexList*, a list of values corresponding to a specific date throughout the project, ending today###
def getIndexList():
    # go from the start date to now
    # gets today's datetime
    now = str(datetime.datetime.now())
    date, time = now.split(" ")
    year, month, day = date.split("-")
    year = year[2:]
    hour, minute, second = time.split(":")
    second = second[:2]

    # gives todays date a value, or endIndex number
    endIndex = datetimeToIndex(year, month, day, hour, minute, second)
    endIndex = round(endIndex / dayToIndexRatio) * dayToIndexRatio

    # calculates the number of entries (numIndices) since the project start date
    diff = endIndex - startIndex
    numIndices = diff / dayToIndexRatio

    # creates the indexList by multiplying the ith entry by the dayToIndexRatio one by one and making the list.
    indexList = []
    for i in range(0, int(numIndices)):
        newVal = startIndex + (i * dayToIndexRatio)
        indexList.append(newVal)

    return indexList

###Takes *indexList* and applies datetime to each of the indicies###
###Starts 10/1/18(m:d:y) at midnight and makes entries on 15 minute intervals###
def getDateList(indexList):
    dateList = []
    # Takes indexList and applies a datetime to each of the index values, starts at 10/1/18(m:d:y) and adds on 15min intervals
    for index in indexList:
        year, month, day, hour, minute, second = indexToDatetime(index, startYear)
        datetime = str(year) + "-" + str(month) + "-" + str(day) + " " + str(hour) + ":" + str(minute) + ":" + str(
            second)
        dateList.append(datetime)
    return dateList

###Creates *indexInWaterYearList*, a new index that is based on water year and only goes to 365 (or 366 on leap year)###
def getIndexInWaterYearList(dateList, indexList):
    if len(dateList) > 0:
        indexInWaterYearList = []
        waterYears = []
        previousWaterYear = getWaterYearFromDate(dateList[0])
        subractionValue = 0

        # Gets date and index number from ith entry, then calculates water year in getWaterYearFromDate
        for i in range(len(indexList)):
            date = dateList[i]
            index = indexList[i]
            waterYear = getWaterYearFromDate(date)

            # making sure it's not a leap year
            if waterYear != previousWaterYear:
                if previousWaterYear % 4 == 0:
                    numDaysInYear = 366
                else:
                    numDaysInYear = 365
                subractionValue += numDaysInYear

            # Making new index that is based on the water year calculated above, index only goes to 365 or 366 days for the year
            indexInWaterYearList.append(index - subractionValue)
            waterYears.append(waterYear)

            previousWaterYear = waterYear
    return indexInWaterYearList, waterYears

###Water year starts as 19, and moves to next water year every october###
def getWaterYearFromDate(date):
    date, time = date.split(" ")
    year, month, date = date.split("-")
    year = int(year)
    month = int(month)
    # if I understand correctly, the water "year" restarts in October, so if the month comes back October or later, its a year more than the actual date.
    if month >= 10:
        year += 1
    waterYear = year
    return waterYear

#----------------------------------------------------------------------#
#-----------------------MAKE TIME SERIES SITE DF-----------------------#
#----------------------------------------------------------------------#

##Adds all tests onto *dischargeDfDict* with cursors, then creates sites *df*###
def makeTimeSeriesSiteDF(cursor, siteID, nbsNum, citSciNum, testsDict, optionsDict):
    indexList = getIndexList()
    dateList = getDateList(indexList)
    indexInWaterYear, waterYear = getIndexInWaterYearList(dateList, indexList)

    dischargeDfDict = {
        "index": indexList,
        "indexInWaterYear": indexInWaterYear,
        "waterYear": waterYear,
        "datetime": dateList
    }

    if testsDict["fieldSheetInfo"]:
        # try:
        fieldSheetDict = getFieldSheetInfo(cursor, siteID, nbsNum, citSciNum)
        dischargeDfDict = joinDict(fieldSheetDict, dischargeDfDict)
    # except:
    #     print("ERROR")
    if testsDict["hoboPressure"] or optionsDict["calculateDischarge"]:
        # try:
        pDict = getP(cursor, siteID)
        dischargeDfDict = joinDict(pDict, dischargeDfDict)
    # except:
    #     print("ERROR")
    if testsDict["hoboLight"]:
        # try:
        lightDict = getLightHobo(cursor, siteID)
        dischargeDfDict = joinDict(lightDict, dischargeDfDict)
    # except:
    #     print("error")
    if testsDict["hoboConductivity"]:
        # try:
        condDict = getConductivityHobo(cursor, siteID)
        dischargeDfDict = joinDict(condDict, dischargeDfDict)
    # except:
    #     print("error")
    if testsDict["hoboOxygen"]:
        # try:
        oxygenDict = getOxygenHobo(cursor, siteID)
        dischargeDfDict = joinDict(oxygenDict, dischargeDfDict)
    # except:
    #     print("error")
    if testsDict["measuredDischarge"]:
        # try:
        qDict = getQ(cursor, siteID)
        dischargeDfDict = joinDict(qDict, dischargeDfDict)
    # except:
    #     print("error")
    if testsDict["hanna"]:
        try:
            hannaDict = getHanna(cursor, siteID)
            dischargeDfDict = joinDict(hannaDict, dischargeDfDict)
        except:
            print("error")
    if testsDict["eureka"]:
        # try:
        eurekaDict = getEureka(cursor, siteID)
        dischargeDfDict = joinDict(eurekaDict, dischargeDfDict)
    # except:
    #     print("error")
    if testsDict["elementar"]:
        # try:
        elementarDict = getElementar(cursor, siteID, nbsNum, citSciNum)
        dischargeDfDict = joinDict(elementarDict, dischargeDfDict)
    # except:
    #     print("error")
    if testsDict["scanCalculated"]:
        # try:
        scanParDict = getScanPar(cursor, siteID, nbsNum, citSciNum)
        dischargeDfDict = joinDict(scanParDict, dischargeDfDict)
    # except:
    #     print("error")
    if testsDict["scanRaw"]:
        # try:
        scanFPDict = getScanFp(cursor, siteID, nbsNum, citSciNum)
        dischargeDfDict = joinDict(scanFPDict, dischargeDfDict)
    # except:
    #     print("error")
    if testsDict["ic"]:
        # try:
        icCationDict = getICCation(cursor, siteID, nbsNum, citSciNum)
        dischargeDfDict = joinDict(icCationDict, dischargeDfDict)
    # except:
    #     print("error")
    if testsDict["ic"]:
        # try:
        icAnionDict = getICAnion(cursor, siteID, nbsNum, citSciNum)
        dischargeDfDict = joinDict(icAnionDict, dischargeDfDict)
    # except:
    #     print("error")
    if testsDict["icp"]:
        # try:
        icpDict = getICP(cursor, siteID, nbsNum, citSciNum)
        dischargeDfDict = joinDict(icpDict, dischargeDfDict)
    # except:
    #     print("error")

    dischargeDf = pd.DataFrame.from_dict(dischargeDfDict)
    if optionsDict["include_batch_id"]:
        pass
    elif "batch_id" in dischargeDf.columns:
        dischargeDf = dischargeDf.drop("batch_id", axis=1)
        ###
    return dischargeDf
