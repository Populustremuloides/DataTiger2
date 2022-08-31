from DataTiger2.DownloadData.DownloadTimeSeries import *
from DataTiger2.USGS_downloaders.scrape_usgs_catchments import *
from DataTiger2.DownloadData.GetTimeSeriesDF import *
import redis
import pyarrow as pa
from datetime import timedelta as td

#-----------------------PROCESSDFSTANDARDCURVE()-----------------------#

###This does a lot of the computing/calls a lot for downloadStandardCurve()###
###Needs to be vastly broken up and simplified###
def processDFStandardCurve(siteID, outputPath, testsDict, optionsDict, calculated_pdf, stationToPriority):
    print(f"started {siteID}")
    #Accesses the redis cache client
    redisClient = redis.Redis(host='localhost', port=6379, db=0)

    #Creates cursor object
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

    #attempts to pull siteID's cache
    siteIDDischargeKey = f"{siteID}FilteredDischarge"
    siteIDSCKey = f"{siteID}Discharge"
    siteIDDischargeCache = redisClient.get(siteIDDischargeKey)
    siteIDStandardCurveCache = redisClient.get(siteIDSCKey)
    try:
        #If the cache is full, it will download that to the folder
        siteIDDischargeCache = pa.deserialize(siteIDDischargeCache)
        siteIDStandardCurveCache = pa.deserialize(siteIDStandardCurveCache)
        print(f"{siteID}Discharge was in cache")
        standardCurveFolder = "Pressure"
        siteIDDischargeCache.to_csv(f"{outputPath}/{siteID}/all_discharge.csv")
        siteIDStandardCurveCache.to_csv(f"{outputPath}/{siteID}/{standardCurveFolder}/StandardizedWaterPressure.csv")
    except:
        #If the cache has expired, or is empty, it follows through with the rest of the code
        print(f"{siteID} Cache empty")

        # Web scraping discharge data from USGS website
        # NOTE: not all of the catchments have data going up until 2021 (this is corroborated across the usgs website)
        #catchments_df, sites_dict = get_usgs_discharge_sites()

        if optionsDict["calculateStandardCurve"]:
            testsDict["hoboPressure"] = True
            testsDict["measuredDischarge"] = True

        ### update testsDict (options?) to grab batch # from database, when batch numbers switch,
        siteDF = makeStandardCurveSiteDF(cursor, siteID, testsDict, optionsDict)
        # Format dates inside df into datetime objects
        siteDF = format_df_datetime(siteDF, 'datetime')

        if optionsDict["include_batch_id"]:
            # NORMALIZE PRESSURE HOBO while making it numeric
            siteDF['pressure_hobo'] = pd.to_numeric(siteDF['pressure_hobo'], errors='coerce')
            # df['pressure_hobo'] = (df['pressure_hobo'] - df['pressure_hobo'].mean()) / (df['pressure_hobo'].std())

            # following function will enforce continuity by aligning each of the beginning and lagging ends of each batch in the pressure data
            siteDF = correct_sensor_gaps(siteDF)

        #groups = siteDF.groupby('batch_id')
        #for name, group in groups:
            #plt.plot(group['index'], group.corrected_values, lw=.4, zorder=4, )

        #plt.title(siteID)
        try:

            #usgs_site = sites_dict[siteID]

            # catchments_df[usgs_site] = catchments_df[usgs_site][catchments_df[usgs_site].date > start_datetime]
            #normalized_usgs = (catchments_df[usgs_site].flows - catchments_df[usgs_site].flows.mean()) / (catchments_df[usgs_site].flows.std())

            siteDF['pressure_hobo'] = siteDF['corrected_values']
            # Separate DF into continuous segments
            siteHoboPressureDf, siteBaroPressureDf, pairings = segment_df_by_continuity(siteDF, calculated_pdf)
            #graphing pressure by time ?
            if siteHoboPressureDf is not None:
                print(f"{siteID}HoboPressureDf is not none")
                #plt.figure(figsize=(17, 7))
                #plt.style.use('ggplot')
                #plt.ylabel("Pressure")
                #plt.xlabel("Time")

                #z = 0
                #for d in siteHoboPressureDf:
                    #z = z + 1
                    #plt.plot(d["index"], d["pressure_hobo"], lw=.3, zorder=2, label=f"{z}")

                dfFilteredByDischarge = siteDF[~siteDF["discharge_measured"].isna()]
                #plt.scatter(dfFilteredByDischarge["index"], dfFilteredByDischarge["pressure_hobo"], s=10, c='tomato', zorder=10, label="discharge measurements")

                #plt.text(0.95, 0.01, f'{str(len(dfFilteredByDischarge["index"].values.tolist()))} discharge measurements at this site.', verticalalignment='bottom', horizontalalignment='right', fontsize=15)
                #plt.title(f"{siteID} Segmented by Chunk of Workable Continuous Pressure Data (gaps < 3 hrs)")
                #plt.legend()
                #plt.savefig(f"{outputPath}/{siteID}/{siteID}_segmented_by_chunk.png", dpi=300) #this figure is what is ending up in the export folders
                #plt.clf()
                #plt.close()

                appendedDischargeData = []
                for i in range(len(siteHoboPressureDf)):
                    #returns empty dfs for now if there isn't a barametric pressure site
                    correctedPressureDf, combinedPressureDf = get_discharge_to_pressure(siteHoboPressureDf[i], siteID, siteBaroPressureDf[i], stationToPriority)
                    if correctedPressureDf.empty & combinedPressureDf.empty:
                        print(f"{siteID}correctedPressureDf and combinedPressureDf are empty")
                    else:
                        appendedDischargeData.append(correctedPressureDf)
                        #list_df hobo pressure
                            #list_pdf_barometric pressure

                            #usgs_site = sites_dict[siteID]

                            #usgs_site = sites_dict[siteID]

                            #catchments_df[usgs_site] = catchments_df[usgs_site][catchments_df[usgs_site].date > start_datetime]
                            #catchments_df[usgs_site] = catchments_df[usgs_site][catchments_df[usgs_site].date < end_datetime]

                            #normalized_usgs = (catchments_df[usgs_site].flows - catchments_df[usgs_site].flows.mean()) / (catchments_df[usgs_site].flows.std())

                            # noDischargeData = (df1['discharge_measured'].isnull().all()) #this is true if all entries for discharge measured are na, I don't think it's ever making it here
                            # if df1 is not None and df2 is not None and len(df1.index) != 0 and len(df2.index) != 0 and not(noDischargeData):
                            #plotRatingCurve(df1, outputPath, siteID, start_date, end_date,i)

                appendedDischargeData = pd.concat(appendedDischargeData)

            else:
                print(f"{siteID}HoboPressureDf is none")

        except:
            print(f"Exception: {siteID} was not found in the sites dict")


        if siteHoboPressureDf is not None:
            standardCurveFolder = "Pressure"

            # This will create the site's cache for discharge and for pressure and set and expiration date to 7 days
            # After the 7 days, it will delete and will, in effect, "refresh" the cache
            redisClient.set(siteIDDischargeKey, pa.serialize(dfFilteredByDischarge).to_buffer().to_pybytes())
            redisClient.expire(siteIDDischargeKey, td(days=7))
            redisClient.set(siteIDSCKey, pa.serialize(appendedDischargeData).to_buffer().to_pybytes())
            redisClient.expire(siteIDSCKey, td(days=7))

            #downloads the CSV's
            dfFilteredByDischarge.to_csv(f"{outputPath}/{siteID}/all_discharge.csv")
            appendedDischargeData.to_csv(f"{outputPath}/{siteID}/{standardCurveFolder}/StandardizedWaterPressure.csv")
            print(f"{siteID} downloaded and cached")
        else:
            print(f"{siteID} not downloaded")

##############################################################################

###Takes *df*, looks forward/backwards on batches, and cleans NA's and outliers###
def correct_sensor_gaps(df):
    df["lock_corrections"] = [0] * len(df[df.columns[0]])
    df["corrections"] = [0] * len(df[df.columns[0]])
    df["corrected_values"] = [None] * len(df[df.columns[0]])
    last_index = 0

    # chooses how many steps to look forward/backward in determining average correction to apply
    interval = 12

    # finds unique batches and uses them to determine where to apply corrections
    batch_switches = df.drop_duplicates(subset='batch_id', keep='first')
    index_switches = batch_switches.index.tolist()

    # Looks at previous and next x values of batches, pressure points, and indices
    for i in range(len(index_switches)):
        item = index_switches[i]
        prev_b = df["batch_id"][item - interval:item].tolist()
        next_b = df["batch_id"][item:item + interval].tolist()

        prev = df["pressure_hobo"][item - interval:item].tolist()
        next = df["pressure_hobo"][item:item + interval].tolist()

        prev_i = df[item - interval:item].index.tolist()
        next_i = df[item:item + interval].index.tolist()

        # If it's not empty, move forward with comparison
        if not all([pd.isna(elem) for elem in prev_b]) and not all([pd.isna(elem) for elem in next_b]):

            # Removes na values from previous arrays
            new_array = []
            new_indices = []
            for j in range(len(prev)):
                if not pd.isna(prev[j]) and prev[j] != "":
                    new_array.append(float(prev[j]))
                    new_indices.append(float(prev_i[j]))
            prev = new_array
            prev_i = new_indices

            # Removes na values from next arrays
            new_array = []
            new_indices = []
            for j in range(len(next)):
                if not pd.isna(next[j]) and next[j] != "":
                    new_array.append(float(next[j]))
                    new_indices.append(float(next_i[j]))
            next = new_array
            next_i = new_indices

            # Feel free to correct me here but I reasoned that extreme outliers happening at the batch number switch are unlikely to be valid, thus I throw some away here
            outliers, outlier_indices = detect_outlier(prev, prev_i)
            if len(outliers) > 0:
                for x in outlier_indices:
                    df.at[x, "pressure_hobo"] = np.nan
            prev_no_outliers = list(set(prev) - set(outliers))
            avg_prev = np.nanmean(prev_no_outliers)

            # Feel free to correct me here but I reasoned that extreme outliers happening at the batch number switch are unlikely to be valid, thus I throw some away here
            outliers, outlier_indices = detect_outlier(next, next_i)
            if len(outliers) > 0:
                for x in outlier_indices:
                    df.at[x, "pressure_hobo"] = np.nan
            next_no_outliers = list(set(next) - set(outliers))
            avg_next = np.nanmean(next_no_outliers)

            # Finally, calculate correction value
            correction = avg_next - avg_prev
            if pd.isna(correction):
                print("OH NO :(")
            if abs(correction) > 100:
                print(f'large correction: {correction}')

            # Apply correction value to all previous pressure points thus reached
            mask = (df.index < index_switches[i])
            df['temp_corrections'] = df['corrections'].where(~mask, correction)
            df['corrections'] = df['corrections'] + df['temp_corrections']
            df["temp_corrections"] = [0] * len(df[df.columns[0]])
        else:
            mask = (last_index <= df.index) & (df.index < index_switches[i])
            df['lock_corrections'] = df['lock_corrections'].where(~mask, df['corrections'])
            df["corrections"] = [0] * len(df[df.columns[0]])
            last_index = index_switches[i]

    df['lock_corrections'] = df['lock_corrections'].where(~df['pressure_hobo'].isna(), None)
    df['corrected_values'] = pd.to_numeric(df['lock_corrections']) + pd.to_numeric(df['pressure_hobo'])

    return df

###Helps *correct_sensor_gaps* to take out outliers from when sensors are pulled and log before turned off###
def detect_outlier(data, indices):
    outliers = []
    outlier_indices = []
    ##### change threshold? Ask Brian
    threshold = 2.5
    mean_1 = np.mean(data)
    std_1 = np.std(data)

    for i in range(len(data)):
        y = data[i]
        index = indices[i]
        z_score = (y - mean_1) / std_1
        if np.abs(z_score) > threshold:
            outliers.append(y)
            outlier_indices.append(index)

    return outliers, outlier_indices

##############################################################################

###Corrects indices by taking out NA's and empty values###
###Corrects *df*, *pdf*, and pairs them to new corrected indices as *list_df*, *list_pdf*###
def segment_df_by_continuity(df, pdf):
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

    try:
        # add beginning and closing index to starts
        starts.insert(0, indices_no_na.index.tolist()[0])
        starts.append(indices_no_na.index.tolist()[-1])
    except:
        print(traceback.format_exc())
        print(f"length of df: {len(df[~df['pressure_hobo'].isna()]['pressure_hobo'].values.tolist())}")
        return None, None, None

    final = sorted(ends+starts)

    # use indices of indices_no_na to find corresponding target df indices
    final = [indices_no_na.loc[i] for i in final]
    len_final = len(final)
    if len_final % 2 != 0:
        print('it should really be even, mayday!! 😰')

    # pair those indices and you're done :)
    pairings = [[final[i], final[i+1]] for i in range(0, len(final), 2)]

    # This line is obnoxiously obtuse but I don't have the bandwidth to simplify rn. Basically it takes the column ['index'] and uses it to find the actual indices from df
    # But it works!
    siteHoboPressureDf = [df[df.loc[df['index'] == pair[0]].index.tolist()[0]:df.loc[df['index'] == pair[1]].index.tolist()[0]] for pair in pairings]
    siteBaroPressureDf = [pdf[df.loc[df['index'] == pair[0]].index.tolist()[0]:df.loc[df['index'] == pair[1]].index.tolist()[0]] for pair in pairings]
    return siteHoboPressureDf, siteBaroPressureDf, pairings

##############################################################################

###Creates *combinedPressureDf* which includes every sites BaroPress and corrections###
###Creates *correctedPressureDf* which includes water pressure and corrected pressure, basically more simplified than above variable###
def get_discharge_to_pressure(siteHoboPressureDf, siteID, siteBaroPressureDf, stationToPriority):

    # list_df hobo pressure
    # list_pdf barometric pressure
    #correcting or discounting barometric pressure
    try:
        #merging dataframe with pressure_hobo data column with _barometricPressure_hanna dataframe on index
        combinedPressureDf = siteHoboPressureDf.merge(siteBaroPressureDf, on='index')

        #while the next site in stationToPriority does not have barometric pressure, increment to the next one
        i = 0
        while not(f"{stationToPriority[siteID][i]}_barometricPressure_hanna" in combinedPressureDf.columns):
            i = i + 1

        #setting a reference site ID to the closest site to our original site with a barometric pressure column
        refSiteID = stationToPriority[siteID][i]

        # ISSUE: some sites don't have {siteID}_barometricPressure_hanna, need to replace that with one nearest to it (use pre-existing function), stationtopriority from get closest stations too
        # making corrected column by subtracting atmospeheric pressure from hobo pressure, this gets us just water pressure
        combinedPressureDf['water_pressure'] = combinedPressureDf['pressure_hobo'] - combinedPressureDf[f"{refSiteID}_barometricPressure_hanna"]

        correctedPressureDf = siteHoboPressureDf.reset_index()

        combinedPressureDf['standardized_water_pressure'] = (combinedPressureDf['water_pressure'] - combinedPressureDf['water_pressure'].mean()) / (combinedPressureDf['water_pressure'].std())

        #creating corrected pressure columns
        correctedPressureDf['standardized_water_pressure'] = combinedPressureDf['standardized_water_pressure']
        correctedPressureDf['water_pressure'] = combinedPressureDf['water_pressure']

        #Cleaning unneeded columns from correctedPressureDf
        correctedPressureDf = correctedPressureDf.drop(columns=['level_0', 'batch_id', 'lock_corrections', 'corrections', 'corrected_values', 'temp_corrections', 'discharge_measured'])
        return correctedPressureDf, combinedPressureDf
    except:
        print(traceback.format_exc())
        print('okay')
        return pd.DataFrame(), pd.DataFrame()
