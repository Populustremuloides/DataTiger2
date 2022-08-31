import seaborn as sns  # for sample data
import sqlite3
from scipy.optimize import curve_fit
from scipy import stats
from datetime import datetime
import copy
import random
from statistics import *
import platform
from DownloadData.SQLQueries import *
import pandas as pd
from DownloadData.DateToIndex import *
from DownloadData.GetTimeSeriesDF import *
from DownloadData.processDFStandardCurve import *
from DownloadData.processDFTimeSeries import *
import numpy as np
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
import traceback
import math
import os
from USGS_downloaders.scrape_usgs_catchments import *
import multiprocessing

#---------------------------------------------------------------------#
#-----------------------DOWNLOAD STANDARD CURVE-----------------------#
#---------------------------------------------------------------------#
###Creates CSV's for each site that include pressure, barometric pressure, and discharge###
def downloadStandardCurve(outputPath, testsDict, optionsDict, cursor):
    # START
    pdf = getAllHannaPressuresDF(cursor)
    pdf = format_df_datetime(pdf, 'datetime')
    xdict, ydict = getSiteCoordinateDicts(cursor)
    stationToPriority = getClosestStationsDict(xdict, ydict)

    pdf_sites = pdf.drop(['waterYear', 'index', 'indexInWaterYear', 'datetime'], axis=1)

    # remove values for pressure under 100 torr (arbitrarily, happy to change it but I feel relatively safe it wouldn't be below 100)
    for col in pdf_sites:
        pdf_sites[col] = pdf_sites[col].where(pdf_sites[col] > 100, None)

    ## This was for testing where you get unlikely jumps in pressure (ie SFL was going from 600 to then flat lining at 0 for month)
    # differences = pdf_sites['SFL_barometricPressure_hanna'].diff()
    # jumps = abs(differences) > 200
    # where_true = jumps.index[jumps == True].tolist()

    mean_series = pdf_sites.mean(axis=1, skipna=True, numeric_only=True)
    mean_per_site = pdf_sites.mean(axis=0, skipna=True, numeric_only=True)

    overall_mean = mean(mean_series[~mean_series.isna()].values.tolist())
    mean_series = mean_series - overall_mean
    mean_per_site_dict = mean_per_site.to_dict()
    calculated_pdf = {}

    for k, v in mean_per_site_dict.items():
        calculated_pdf[k] = v + mean_series

    calculated_pdf = pd.DataFrame(calculated_pdf)
    calculated_pdf['datetime'] = pdf['datetime']
    calculated_pdf['index'] = pdf['index']
    calculated_pdf['indexInWaterYear'] = pdf['indexInWaterYear']
    calculated_pdf['waterYear'] = pdf['waterYear']

    # END

    siteListTable = "SELECT * FROM master_site"
    cursor.execute(siteListTable)
    result = cursor.fetchall()

    for line in result:
        siteID = line[3]

        # Checks for site name file/makes it, checks for Discharge file/makes it
        pressurePath = os.path.join(outputPath, siteID)
        if siteID != "":
            old_output_path = copy.copy(outputPath)
            if not os.path.isdir(os.path.join(outputPath, siteID)):
                os.mkdir(os.path.join(outputPath, siteID))
                outputPath = old_output_path
                os.mkdir(os.path.join(pressurePath, "Pressure"))
        standardCurveFolder = "Pressure"
        if os.path.exists(f"{pressurePath}/{standardCurveFolder}"):
            pass
        else:
            os.mkdir(os.path.join(pressurePath, "Pressure"))

    print("Directory created")
        ###If you want a site-by-site look at how the code works with stops, you can comment this in first.
        ###Now multiprocessDFStandardCurve() is prioritized
        #processDFStandardCurve(siteID, outputPath, testsDict, optionsDict, outputPath, calculated_pdf, stationToPriority)

    multiprocessDFStandardCurve(testsDict, optionsDict, outputPath, calculated_pdf, stationToPriority, result)

    return "successfully downloaded standard curve report to " + outputPath

#############################################################

def multiprocessDFStandardCurve(testsDict, optionsDict, outputPath, calculated_pdf, stationToPriority, result):
    #Starts by creating a list(siteIDList) of tuples(site_args) to be sent into the multiprocessing pools
    #I omitted SFU, SAN, and processing the NBS sites as those do not have data for pressure or discharge.
    siteIDList = []
    for line in result:
        if line[3] != "":
            siteID = line[3]
            if siteID == "SFU" or siteID == "SAN":
                print("skip these bad boys")
            else:
                site_args = (siteID, outputPath, testsDict, optionsDict, calculated_pdf, stationToPriority)
                siteIDList.append(site_args)
        else:
            print("NBS Site")

    #Calculates the amount of CPU's available on the system, then starmap will evenly batch and divide workloads onto each CPU
    p = multiprocessing.Pool()
    p.starmap(processDFStandardCurve, siteIDList)

##############################################################################

####does not download/note being used####
###Creates ggplot of [site]water pressure###
###Discrepancies in dataframes used to make the graphs###
#def plotRatingCurve(df, outputPath, siteID, start_date, end_date,iteration):
    #iteration=iteration+1

    #filteredDF = df.loc[(~df.discharge_measured.isna()) & (~df.corrected_pressure_hobo.isna())]
    #x = df['discharge_measured'].values.tolist()
    #y = df['corrected_pressure_hobo'].values.tolist()

    #x_as_array = np.asarray(filteredDF['discharge_measured'].values).reshape((-1, 1))
    #y_as_array = np.asarray(filteredDF['corrected_pressure_hobo'].values)
    #not currently using this, the sets are really small
    #x_train, x_test, y_train, y_test = train_test_split(x_as_array, y_as_array, train_size=0.7, test_size=0.3, random_state=100)

    #model = LinearRegression()
    #model.fit(x_as_array, y_as_array)

    #intercept = model.intercept_
    #slope = model.coef_
    #range = np.linspace(min(x_as_array), max(x_as_array), 10)

    #plt.figure(figsize=(17, 7))
    #plt.style.use('ggplot')
    #plt.scatter(x, y, c="blue")
    #plt.plot(range, intercept + slope * range)
    #plt.ylabel("Water Pressure ")
    #plt.xlabel("Discharge")
    #plt.title(f"{siteID} Section {iteration} Rating Curve")
    #plt.legend()
    #plt.savefig(f"{outputPath}/{siteID}/{siteID}_{start_date}_to_{end_date}_rating_curve.png", dpi=300)
    #plt.close()

    #attempt at linear regression  plot
    #try:
        # res = stats.linregress(df["corrected_pressure_hobo"], df["discharge_measured"])
        # plt.plot(df["corrected_pressure_hobo"], res.intercept + res.slope * df["corrected_pressure_hobo"], 'r', c='blue', label='fitted line',title='LOOK AT ME')
    # except:
    #     print(traceback.format_exc())

    # plt.xlabel("Barometric Discounted Pressure")
    # plt.ylabel("Discharge")

    # plt.scatter(df["corrected_pressure_hobo"], df["discharge_measured"], s=6, c="tomato", zorder=4, label=f"discharge")
    # plt.legend()
    # plt.savefig(f"{outputPath}/{siteID}/{siteID}_{start_date}_to_{end_date}_rating_curve.png", dpi=300)
    # plt.clf()
    # plt.close()

#------------------------FORMAT_DF_DATETIME()------------------------#

###Corrects *df* datetime, to '%y-%m-%d %H:%M:%S'###
def format_df_datetime(df, name_of_datetime):
    df[name_of_datetime] = df[name_of_datetime].apply(lambda x: " ".join(
        ["-".join(list(map(lambda y: y.zfill(2), x.split(" ")[0].split("-")))),
         ":".join(list(map(lambda y: y.zfill(2), x.split(" ")[1].split(":"))))]))
    df[name_of_datetime] = pd.to_datetime(df.datetime, format='%y-%m-%d %H:%M:%S')
    return df

#--------------------------------------------------------------------#
#------------------------DOWNLOAD TIME SERIES------------------------#
#--------------------------------------------------------------------#

###This needs a lot of work to do what we want it to###
###Creates CSV for each site of all of the data that's been taken for that site, including lab tests###
def downloadTimeSeries(outputPath, testsDict, optionsDict, cursor):
    print("SYSTEM: ", platform.system())

    if optionsDict["calculateDischarge"]:
        pdf = getAllHannaPressuresDF(cursor)
        xdict, ydict = getSiteCoordinateDicts(cursor)
        stationToPriority = getClosestStationsDict(xdict, ydict)
    else:
        pdf = None
        stationToPriority = None

    siteListTable = "SELECT * FROM master_site"
    cursor.execute(siteListTable)
    result = cursor.fetchall()

    multiprocessDFTimeSeries(result, testsDict, optionsDict, outputPath, pdf, stationToPriority)
    #for line in result:
        #print(f"Now starting {line[3] if line[3] != '' else str(line[2])}")
        #print("[" + (("#") * (progress_length - len(progress_list))) + ((" ") * len(progress_list)) + "]")
        #print(str(round(((((progress_length - len(progress_list)) / (progress_length)) * 100)), 2)) + "% done" + "\n")

        #siteID = line[3]
        #nbsNum = line[2]
        #citSciNum = line[4]

        #nbsNum = nbsNum.split(".")[1]

        # generate the dataframe
        #if optionsDict["includeSynoptic"] == True:
            #timeSeriesDf = processDF(cursor, siteID, nbsNum, citSciNum, testsDict, optionsDict, outputPath, pdf, stationToPriority)
        #else:
            #if siteID != "":
                #timeSeriesDf = processDF(cursor, siteID, nbsNum, citSciNum, testsDict, optionsDict, outputPath, pdf, stationToPriority)
        #progress_list.remove(siteID)
        #print(f"{line[3] if line[3] != '' else str(nbsNum)} complete")

    return "successfully downloaded time series report to " + outputPath


def multiprocessDFTimeSeries(result, testsDict, optionsDict, outputPath, pdf, stationToPriority):
    siteIDList = []
    for line in result:
        if line[3] != "":
            siteID = line[3]
            nbsNum = line[2]
            citSciNum = line[4]
            nbsNum = nbsNum.split(".")[1]

            site_args = (siteID, nbsNum, citSciNum, testsDict, optionsDict, outputPath, pdf, stationToPriority)
            siteIDList.append(site_args)
        else:
            print("NBS Site")
    p = multiprocessing.Pool()
    p.starmap(processDF, siteIDList)

# ----------------------------------------------------------------------#
# -------------------------DOWNLOAD LOGGER GAPS-------------------------#
# ----------------------------------------------------------------------#

###Tell us for each site where we went more than an hour without getting data###
def downloadLoggerGapsReport(outputPath, cursor, testsDict, optionsDict):
    siteListTable = "SELECT * FROM master_site"
    cursor.execute(siteListTable)
    result = cursor.fetchall()

    for line in result:
        print(f"Now starting {line[3] if line[3] != '' else str(line[2])}")

        siteID = line[3]
        nbsNum = line[2]
        citSciNum = line[4]

        nbsNum = nbsNum.split(".")[1]

        if siteID != "":
            dataDict = {}

            df = makeTimeSeriesSiteDF(cursor, siteID, nbsNum, citSciNum, testsDict, optionsDict)
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

                    #keeperValues = values[keeperMask]
                    keeperDates = dates[keeperMask]
                    keeperIndices = indices[keeperMask]

                    if len(keeperIndices) > 0:
                        firstRecordedDate = keeperDates[0]
                        lastRecordedDate = keeperDates[-1]

                    #numPossibleDays = len(keeperIndices) / (4 * 24)

                    for i in range(1, len(keeperIndices)):
                        previous = keeperIndices[i - 1]
                        current = keeperIndices[i]

                        if current - previous > 5:  # if we go for ~ more than an hour without data
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

        # Checks for site name file/makes it, checks for LoggerGaps file/makes it
        gapPath = os.path.join(outputPath, siteID)
        if siteID != "":
            old_output_path = copy.copy(outputPath)
            if not os.path.isdir(os.path.join(outputPath, siteID)):
                os.mkdir(os.path.join(outputPath, siteID))
                outputPath = old_output_path
                os.mkdir(os.path.join(gapPath, "LoggerGaps"))
        if os.path.exists(f"{gapPath}/LoggerGaps"):
            print("Logger gaps folder present")
        else:
            os.mkdir(os.path.join(gapPath, "LoggerGaps"))

        missingDf = pd.DataFrame.from_dict(dataDict)
        loggerGaps = "LoggerGaps"
        missingDf.to_csv(f"{outputPath}/{siteID}/{loggerGaps}/loggerGapsReport_" + siteID + ".csv")
        print("Downloaded")

    return "successfully downloaded logger gaps report to " + outputPath

#-----------------------------------------------------------------------#
#--------------------------MULTI USE FUNCTIONS--------------------------#
#-----------------------------------------------------------------------#

###Creates *stationToClosest*, a sorted list, closest to farthest of each site to other sites###
###There are a lot of other variables created here that I don't think are necessary but I will look at it later###
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

        stationToClosest[station] = priorityList

    return stationToClosest

###Creates *stationToDistances*, takes x/y coordinates and creates list distances from current site to other sites###
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

###Kinda weird function, but cool###
###Creates *dist*, calculates distance of a site to the current site based on x/y coordinates###
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

##############################################################################

###Creates *xdict*, *ydict*, a x/y coordinate list for each site###
def getSiteCoordinateDicts(cursor):
    siteListTable = "SELECT * FROM master_site"
    cursor.execute(siteListTable)
    result = cursor.fetchall()

    xdict = {}
    ydict = {}
    for line in result:

        siteID = line[3]

        if siteID != "":
            xdict[siteID] = line[8]
            ydict[siteID] = line[9]

    return xdict, ydict

#------------------------------------END------------------------------------#

######No idea why we have this######
#def expandIndex(targetIndex, allIndices):
    #newBools = []
    #found_newBool = False
    #for index in allIndices:
        #if abs(index - targetIndex) < 0.1:
            #newBools.append(True)
            #found_newBool = True
        # if the first statement has been reached and passed, you can break the loop :)
        # elif found_newBool:
        #     break
        #else:
            #newBools.append(False)
    #return newBools

# what is the right way to do this?
# calculate a barometric pressure column that I can subtract from the pressure measurements
# subract them
# run the standard curve
# calculate discharge

######No idea why we have this######
#def replaceBlankWithNone(array):
    #array = list(array)
    #for j in range(len(array)):
        #val = array[j]
        #val = str(val)
        #val.replace(" ","")
        #if val == "":
            #array[j] = None
    #return array
