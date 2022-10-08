import pandas as pd
from DownloadData.DownloadTimeSeries import *
from USGS_downloaders.scrape_usgs_catchments import *
from DownloadData.GetTimeSeriesDF import *
import redis
import pyarrow as pa
from datetime import timedelta as td

#---------------------------PROCESSDF()---------------------------#

def processDF(siteID, nbsNum, citSciNum, testsDict, optionsDict, outputPath, pdf, stationToPriority):
    print(f"started {siteID}")
    # Accesses the cache client
    redisClient = redis.Redis(host='localhost', port=6379, db=0)

    # Creates cursor object
    try:
        # open the db file
        filename = os.path.join(os.getcwd(), "DatabaseName.txt")
        with open(filename, "r") as dbNameFile:
            dbName = dbNameFile.read()
            defaultDBFile = dbName
            if defaultDBFile.endswith(".db"):
                conn = sqlite3.connect(defaultDBFile)
                cursor = conn.cursor()
            else:
                print("oops")
    except:
        print("nope didnt open")

    #Creates Time series folders
    timeSeriesPath = os.path.join(outputPath, siteID)
    if siteID != "":
        old_output_path = copy.copy(outputPath)
        if not os.path.isdir(os.path.join(outputPath, siteID)):
            os.mkdir(os.path.join(outputPath, siteID))
            os.mkdir(os.path.join(timeSeriesPath, "TimeSeries"))
    if os.path.exists(f"{timeSeriesPath}/TimeSeries"):
        print("Time series folder present")
    else:
        os.mkdir(os.path.join(timeSeriesPath, "TimeSeries"))
    outputPath = old_output_path

    #attempts to pull siteID's cache
    siteIDTimeSeriesKey = f"{siteID}TimeSeries"
    siteIDTimeSeriesCache = redisClient.get(siteIDTimeSeriesKey)
    try:
        #If the cache is full, it will download that to the folder
        siteIDTimeSeriesCache = pa.deserialize(siteIDTimeSeriesCache)
        print(f"{siteID} Time Series was in cache")
        timeSeries = "timeSeries"
        siteIDTimeSeriesCache.to_csv(f"{outputPath}/{siteID}/{timeSeries}/timeSeriesReport_" + siteID + ".csv")
    except:
        #If the cache has expired, or is empty, it follows through with the rest of the code
        print(f"{siteID} Time Series Cache empty")
        if optionsDict["calculateDischarge"]:
            testsDict["hoboPressure"] = True

        #makes df by running cursor on database of all of the tests done at the site
        timeSeriesDf = makeTimeSeriesSiteDF(cursor, siteID, nbsNum, citSciNum, testsDict, optionsDict)

        # calculate discharge
        if optionsDict["calculateDischarge"]:
            timeSeriesDf = addCalculatedDischarge(timeSeriesDf, siteID, pdf, stationToPriority, cursor)

        # interpolate
        #if optionsDict["interpolate"] == True:
            #df = interpolate(df)

        if optionsDict["correct_values"]:
            sensors = ["Hanna", "YSI"]
            target_list = ["electricalConductivity", "pH", "temperature", "orpMV", "dissolvedOxygen_mgL"]
            corrections_df, device = correctValuesCurve(timeSeriesDf, sensors, target_list)

            if corrections_df is None:
                if siteID == "":
                    print(
                        f"huge error! corrections for NBS{nbsNum} failed because no hobo/eureka/hanna data was found. Possibly it only has scan data?")
                else:
                    print(
                        f"Huge error! Corrections for {siteID} failed because no hobo/eureka/hanna data was found. Possibly it only has scan data?")
                saveDF(timeSeriesDf, outputPath, siteID, nbsNum, siteIDTimeSeriesKey, redisClient)
                print(f"{siteID} downloaded")
            else:
                saveDF(timeSeriesDf, outputPath, siteID, nbsNum, siteIDTimeSeriesKey, redisClient, True, corrections_df, target_list, sensors, device)
                print(f"{siteID} downloaded")
        else:
            saveDF(timeSeriesDf, outputPath, siteID, nbsNum, siteIDTimeSeriesKey, redisClient)
            print(f"{siteID} downloaded")

        return timeSeriesDf

##############################################################################

###This is trying to use rating curves to predict discharge. Doesn't work###
###Consider taking this out as we are doing this in R, unless we want it calculated onto the spreadsheet###
def addCalculatedDischarge(timeSeriesDf, siteID, pdf, stationToPriority, cursor):
    barometricData = getBarometricPressureColumnNoCorrections(siteID, pdf, stationToPriority)
    pressureData = timeSeriesDf["pressure_hobo"]
    dates = timeSeriesDf["datetime"]
    print(len(barometricData))
    print(len(pressureData))

    #trying to see if there are entries at the same times
    mask1 = np.asarray(~barometricData.isna())
    mask2 = np.asarray(~pressureData.isna())
    mask = np.logical_and(mask1, mask2)

    barometricData = np.asarray(barometricData)
    pressureData = np.asarray(pressureData)
    correctedPressureData = pressureData

    correctedPressureData[mask] = float(pressureData[mask]) - float(barometricData[mask])
    correctedPressureData[~mask] = None

    dischargePoints = [None] * len(pressureData)

    keyDict, siteDict = getSlopeInterceptDicts(cursor, siteID)

    for i in range(len(correctedPressureData[mask])):
        date = dates[mask][i]
        slope, intercept = getSlopeIntercept(date, siteID, keyDict, siteDict)

        pressure = correctedPressureData[mask][i]
        discharge = pressure * slope + intercept
        dischargePoints[mask][i] = discharge

    timeSeriesDf["calculated_discharge"] = dischargePoints

    return timeSeriesDf

###Creates a barometric pressure column, not yet added to data###
def getBarometricPressureColumnNoCorrections(siteID, pdf, stationToPriority):
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

###This calculates the equation for the discharge rating curve###
###NOT WORKING###
def getSlopeIntercept(datetime, siteID, keyDict, siteDict):
    date, time = datetime.split(" ")
    year, month, day = date.split("/")
    for key in siteDict.keys():
        vec = siteDict[key]
        slope = vec[keyDict["slope"]]
        intercept = vec[keyDict["intercept"]]
        startIndex = vec[keyDict["startIndex"]]
        endIndex = vec[keyDict["endIndex"]]

        # if
        #     return slope, intercept # FIXME not sure where intercept is coming from

    print("WARNING getSlopeIntercept not implemented")
    return 1, 1

##############################################################################

###Not used rn###
#def interpolate(df):
    #print("interpolation not functional yet")
    #return df

##############################################################################

###No idea lol###
###Seems like it is trying to correct YSI, Hanna, and hobo values based on equations, idk###
def correctValuesCurve(timeSeriesDf, sensors, target_list):
    timeSeriesDf = timeSeriesDf.replace("", np.nan, regex=True)
    timeSeriesDf = timeSeriesDf.fillna(value=np.nan)

    device = senseDeviceType(target_list, timeSeriesDf)
    if device is None:
        return None, None

    # correct df names based on device type
    if device == "hobo":
        timeSeriesDf = timeSeriesDf.rename(columns={"conductivity_hobo": "electricalConductivity_hobo", "dissolvedOxygen_mgl_hobo": "dissolvedOxygen_mgL_hobo"})
        target_list.remove("orpMV")
        target_list.remove("pH")
    elif device == "eureka":
        timeSeriesDf = timeSeriesDf.rename(columns={"orp_eureka": "orpMV_eureka"})
        target_list.remove("dissolvedOxygen_mgL")
    elif device == "hanna":
        target_list.append("dissolvedOxygenPercent")
    else:
        return None, None

    return timeSeriesDf, device
    target_list = target_list
    device = device
    for target in target_list:
        filtered_df = timeSeriesDf[~timeSeriesDf[f"{target}_fieldsheet"].isna()]
        x = filtered_df["index"].tolist()
        y = filtered_df[f"{target}_fieldsheet"].tolist()
        minind = x[0]
        maxind = x[-1]

        if len(x) < 3:
            continue

        popt, _ = curve_fit(objective, x, y)

        timeSeriesDf[f"YSI_curve_{target}"] = objective(timeSeriesDf["index"], *popt)

        # cut off errant tails of curve
        timeSeriesDf[f"YSI_curve_{target}"] = timeSeriesDf[f"YSI_curve_{target}"].where((timeSeriesDf["index"] > minind), None)
        timeSeriesDf[f"YSI_curve_{target}"] = timeSeriesDf[f"YSI_curve_{target}"].where((timeSeriesDf["index"] < maxind), None)

        # filter df for creating residuals between ysi curve and data points, find residual points
        filtered_df = timeSeriesDf[~timeSeriesDf[f"{target}_fieldsheet"].isna()]
        filtered_df = filtered_df[~filtered_df[f"{target}_{device}"].isna()]
        filtered_df = filtered_df[~filtered_df[f"YSI_curve_{target}"].isna()]

        filtered_df[f"residual_curve_{target}"] = [None] * len(filtered_df)
        # fdf["res_curve"] = fdf["YSI_curve"] - fdf["electricalConductivity_eureka"]
        filtered_df[f"residual_curve_{target}"] = filtered_df[f"YSI_curve_{target}"] - filtered_df[f"{target}_{device}"]
        asdf = copy.copy(filtered_df)
        asdf = asdf.rename(columns={f"residual_curve_{target}": f"{target}"})
        asdf = asdf[f"{target}"]
        timeSeriesDf = pd.concat([timeSeriesDf, asdf], axis=1)

        res = filtered_df[f"residual_curve_{target}"].tolist()
        x = filtered_df["index"].tolist()

        # add residual points to df
        filtered_df = filtered_df[f"residual_curve_{target}"]
        timeSeriesDf = pd.concat([timeSeriesDf, filtered_df], axis=1)

        # optimize curve for residual points
        popt, _ = curve_fit(objective, x, res)

        filtered_df = timeSeriesDf[~timeSeriesDf[f"residual_curve_{target}"].isna()]
        x = filtered_df["index"].tolist()
        minind = x[0]
        maxind = x[-1]

        timeSeriesDf[f"residual_curve_{target}"] = objective(timeSeriesDf["index"], *popt)
        timeSeriesDf[f"residual_curve_{target}"] = timeSeriesDf[f"residual_curve_{target}"].where((timeSeriesDf["index"] > minind), None)
        timeSeriesDf[f"residual_curve_{target}"] = timeSeriesDf[f"residual_curve_{target}"].where((timeSeriesDf["index"] < maxind), None)

        timeSeriesDf[f"{target}_{device}_corrected"] = timeSeriesDf[f"{target}_{device}"] + timeSeriesDf[f"residual_curve_{target}"]

    return timeSeriesDf, device
    #whats going on here

###Tells correctValuesCurve() what device's values they're correcting###
def senseDeviceType(target_list, timeSeriesDf):
    parse_df = timeSeriesDf[~timeSeriesDf[f"{target_list[0]}_fieldsheet"].isna()]
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

###This equation is used a lot for the rating curve...###
def objective(x, a, b, c, d, e, f, g, h):
    return (a * x) + (b * x ** 2) + (c * x ** 3) + (d * x ** 4) + (e * x ** 5) + (f * x ** 6) + (g * x ** 7) + h

###No idea at all, expectially with the *objective* above###
def rating_curve_objective(x, a, b):
    # return (a * x) + (b * x ** 2) + c
    return (a * x) + b

##############################################################################

###saves *df* as CSV for downloadTimeSeries()###
def saveDF(timeSeriesDf, outputPath, siteID, nbsNum, siteIDTimeSeriesKey, redisClient,  saveFig=False, corrections_df=None, target_list=None, sensors=None, device=None):
    # This will create the site's cache and set and expiration date to 7 days
    # After the 7 days, it will delete and will, in effect, "refresh" the cache
    redisClient.set(siteIDTimeSeriesKey, pa.serialize(timeSeriesDf).to_buffer().to_pybytes())
    redisClient.expire(siteIDTimeSeriesKey, td(days=7))

    timeSeries = "timeSeries"
    timeSeriesDf.to_csv(f"{outputPath}/{siteID}/{timeSeries}/timeSeriesReport_" + siteID + ".csv")

    #Only need this section because SaveFig runs on this
    if platform.system() == "Windows":
        if siteID != "":
            #filePath = outputPath + "\\timeSeriesReport_" + siteId + ".csv"
            figPath = outputPath + "\\timeSeriesCorrectionFigures\\" + siteID
            figTitle = siteID
        else:
            #filePath = outputPath + "\\timeSeriesReport_NBS" + nbsNum + ".csv"
            figPath = outputPath + "\\timeSeriesCorrectionFigures_NBS\\" + nbsNum
            figTitle = f"NBS_{nbsNum}"
    else:
        if siteID != "":
            #filePath = outputPath + "/timeSeriesReport_" + siteId + ".csv"
            figPath = outputPath + "/timeSeriesCorrectionFigures/" + siteID
            figTitle = siteID
        else:
            #filePath = outputPath + "/timeSeriesReport_NBS" + nbsNum + ".csv"
            figPath = outputPath + "/timeSeriesCorrectionFigures_NBS/" + nbsNum
            figTitle = f"NBS_{nbsNum}"

    #print(filePath)
    #timeSeriesDf.to_csv(filePath, index=False)
    if saveFig:
        saveFigure(corrections_df, figPath, target_list, sensors, figTitle, device)

###Saves figure (if saveFig) as PNG###
def saveFigure(timeSeriesDf, figPath, target_list, sensors, figTitle, device):
   for target in target_list:
        try:
            # save figure
            plt.figure(figsize=(17, 7))
            plt.style.use('ggplot')
            plt.xlabel("Days since 2018")
            plt.ylabel(f"{target}")
            plt.scatter(timeSeriesDf["index"], timeSeriesDf[f"{target}_{device}"], c="grey", s=.5, zorder=1, label="raw data")


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
            plt.scatter(timeSeriesDf["index"], timeSeriesDf[f"{target}_fieldsheet"], s=6, c="tomato", zorder=4,
                        label=f"{target}_YSI")
            plt.axvline(x=945, c="tomato", zorder=7, label="Stopped calibrating May 3, 2021")
            plt.title(f"{figTitle} {target}")
            plt.legend()
            #plt.savefig(f"{figPath}_{target}.png", dpi=300)
            plt.clf()
            plt.close()
        except:
            print(traceback.format_exc())
            print(f"PNG export for {figTitle} failed at target: {target}")
