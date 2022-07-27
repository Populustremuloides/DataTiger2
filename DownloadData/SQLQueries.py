import traceback

from DownloadData.DateToIndex import *

def getDatetime(date, time):
    if "PM" in time:
        hour, minute = time.split(":")
        hour = str(int(hour) + 12)
        time = hour + ":" + minute

    time = time.replace(" AM", "")
    time = time.replace(" PM", "")
    time = time.replace("AM", "")
    time = time.replace("PM", "")

    year, month, day = date.split("-")
    if len(year) == 4:
        year = year[-2:]
    hour, minute = time.split(":")
    second = "0"
    datetime = year + "-" + month + "-" + day + " " + hour + ":" + minute + ":" + second

    return datetime

def splitDatetimeP(datetime):
    # index 0 = January 1, 2019
    date, time = datetime.split(" ")
    try:
        hour, minute, second = time.split(":")
    except:
        hour, minute = time.split(":")
        second = "00"
    month, day, year = date.split("-")

    if int(month) > 12:
        # probably year, month, day
        year1 =  month
        month1 = day
        day1 = year

        year = year1
        month = month1
        day = day1

    if len(year) == 4:
        year = year[-2:]

    # if int(year) <= 18 or int(year) > 20:
        # print(datetime)
    #print(year)

    return year, month, day, hour, minute, second

def splitDatetimeQ(datetime):

    date, time = datetime.split(" ")
    if "/" in date:
        month, day, year = date.split("/")
    else:
        year, month, day = date.split("-")

    hour, minute, second = time.split(":")
    year = year[-2:]

    if int(month) > 12:
        ###
        # probably year, month, day
        year1 = month
        month1 = day
        day1 = year

        year = year1
        month = month1
        day = day1
    return year, month, day, hour, minute, second

def getP(cursor, siteid):
    sqlquery = "SELECT *, MAX(batch_id) FROM (hobo_pressure_logs_1 INNER JOIN hobo_pressure_batches_1 USING(batch_id)) WHERE site_id = ? GROUP BY logging_date, logging_time;"
    sitetuple = (siteid,)
    cursor.execute(sqlquery, sitetuple)
    result = cursor.fetchall()
    dateToData = {}

    dateToData["batch_id"] = []
    dateToData["datetime"] = []
    dateToData["pressure_hobo"] = []
    dateToData["temperature_hobo"] = []
    dateToData["index"] = []

    for item in result:
        batch_id = item[4]
        date = item[0]
        time = item[1]
        pressure = item[2]
        temperature = item[3]
        date = date.split(" ")[0]
        datetime = date + " " + time

        year, month, day, hour, minute, second = splitDatetimeP(datetime)

        if int(month) > 18:
            month1 = day
            year1 = month
            day1 = year

            year = year1
            month = month1
            day = day1


        index = datetimeToIndex(year, month, day, hour, minute, second)
        index = round(index / dayToIndexRatio) * dayToIndexRatio

        dateToData["batch_id"].append(batch_id)
        dateToData["datetime"].append(datetime)
        dateToData["pressure_hobo"].append(pressure)
        dateToData["index"].append(index)
        dateToData["temperature_hobo"].append(temperature)

    return dateToData

def getSlopeInterceptDicts(cursor, siteID):
    sqlquery = "SELECT * FROM standard_curve_slopes WHERE site_id == ?"
    siteTuple = (siteID,)

    cursor.execute(sqlquery, siteTuple)
    result = cursor.fetchall()

    siteToInfoDict = {}

    keyToIndex = {}
    keyToIndex["slope"] = 0
    keyToIndex["intercept"] = 1
    keyToIndex["minPressure"] = 2
    keyToIndex["maxPressure"] = 3
    keyToIndex["startIndex"] = 4
    keyToIndex["endIndex"] = 5

    i = 0
    for item in result:
        site = item[0]
        slope = item[1]
        intercept = item[2]
        minPressure = item[3]
        maxPressure = item[4]
        startDate = item[5]
        startTime = item[6]
        endDate = item[7]
        endTime = item[8]

        syear, smonth, sday = startDate.split("-")
        shour, sminute, ssecond = startDate.split(":")

        eyear, emonth, eday = startDate.split("-")
        ehour, eminute, esecond = startDate.split(":")

        startIndex = datetimeToIndex(syear, smonth, sday, shour, sminute, ssecond)
        endIndex = datetimeToIndex(eyear, emonth, eday, ehour, eminute, esecond)
        # FIXME: allow multiple entries per site
        # FIXME: do as a dataframe

        siteToInfoDict[site + "_" + str(i)] = [slope, intercept, minPressure, maxPressure, startIndex, endIndex]
        i = i + 1
    return keyToIndex, siteToInfoDict

def getLightHobo(cursor, siteid):
    sqlquery = "SELECT *, MAX(batch_id) FROM (hobo_light_logs_1 INNER JOIN hobo_light_batches_1 USING(batch_id)) WHERE site_id = ? GROUP BY logging_date, logging_time;"
    sitetuple = (siteid,)
    cursor.execute(sqlquery, sitetuple)
    result = cursor.fetchall()
    dateToData = {}
    dateToData["batch_id"] = []
    dateToData["datetime"] = []
    dateToData["intensity_hobo"] = []
    dateToData["temperature_hobo"] = []
    dateToData["index"] = []

    for item in result:
        date = item[0]
        time = item[1]
        intensity = item[3]
        batch_id = item[4]
        temperature = item[2]
        date = date.split(" ")[0]
        datetime = date + " " + time

        year, month, day, hour, minute, second = splitDatetimeP(datetime)
        index = datetimeToIndex(year, month, day, hour, minute, second)
        index = round(index / dayToIndexRatio) * dayToIndexRatio

        dateToData["batch_id"].append(batch_id)
        dateToData["datetime"].append(datetime)
        dateToData["intensity_hobo"].append(intensity)
        dateToData["index"].append(index)
        dateToData["temperature_hobo"].append(temperature)

    return dateToData

def getConductivityHobo(cursor, siteid):
    sqlquery = "SELECT *, MAX(batch_id) FROM (hobo_conductivity_logs_1 INNER JOIN hobo_conductivity_batches_1 USING(batch_id)) WHERE site_id = ? GROUP BY logging_date, logging_time;"
    sitetuple = (siteid,)
    cursor.execute(sqlquery, sitetuple)
    result = cursor.fetchall()
    dateToData = {}
    dateToData["batch_id"] = []
    dateToData["datetime"] = []
    dateToData["conductivity_hobo"] = []
    dateToData["temperature_hobo"] = []
    dateToData["index"] = []

    for item in result:
        batch_id = item[0]
        ###
        date = item[0]
        time = item[1]
        conductance = item[3]
        temperature = item[2]
        date = date.split(" ")[0]
        datetime = date + " " + time

        year, month, day, hour, minute, second = splitDatetimeP(datetime)
        index = datetimeToIndex(year, month, day, hour, minute, second)
        index = round(index / dayToIndexRatio) * dayToIndexRatio
        dateToData["batch_id"].append(batch_id)
# n
        dateToData["datetime"].append(datetime)
        dateToData["conductivity_hobo"].append(conductance)
        dateToData["index"].append(index)
        dateToData["temperature_hobo"].append(temperature)

    return dateToData

def getOxygenHobo(cursor, siteid):
    sqlquery = "SELECT *, MAX(batch_id) FROM (hobo_oxygen_logs_1 INNER JOIN hobo_oxygen_batches_1 USING(batch_id)) WHERE site_id = ? GROUP BY logging_date, logging_time;"
    sitetuple = (siteid,)
    cursor.execute(sqlquery, sitetuple)
    result = cursor.fetchall()
    dateToData = {}
    dateToData["batch_id"] = []
    dateToData["datetime"] = []
    dateToData["dissolvedOxygen_mgl_hobo"] = []
    dateToData["temperature_hobo"] = []
    dateToData["index"] = []
    dateToData["filename"] = []

    for item in result:
        date = item[0]
        time = item[1]
        dissolvedOxygen = item[3]
        batch_id = item[4]
        temperature = item[2]
        date = date.split(" ")[0]
        datetime = date + " " + time
        filename = item[-5]

        year, month, day, hour, minute, second = splitDatetimeP(datetime)
        index = datetimeToIndex(year, month, day, hour, minute, second)
        index = round(index / dayToIndexRatio) * dayToIndexRatio

        dateToData["batch_id"].append(batch_id)
        dateToData["datetime"].append(datetime)
        dateToData["dissolvedOxygen_mgl_hobo"].append(dissolvedOxygen)
        dateToData["index"].append(index)
        dateToData["temperature_hobo"].append(temperature)
        dateToData["filename"].append(filename)
    return dateToData

def getHanna(cursor, siteid):
    sqlquery = "SELECT *, MAX(hanna_batch) FROM (hanna_reads INNER JOIN hanna_batches ON hanna_reads.hanna_batch = hanna_batches.hanna_batch_id) WHERE site_id = ? GROUP BY logging_date, logging_time, temperature, pH, orp_mv, ec, pressure, do_percent, do_concentration, hanna_reads.remarks ORDER BY logging_date, logging_time;"
    sitetuple = (siteid,)
    cursor.execute(sqlquery, sitetuple)
    result = cursor.fetchall()
    dateToData = {}
    dateToData["batch_id"] = []
    dateToData["datetime"] = []
    dateToData["index"] = []
    dateToData["temperature_hanna"] = []
    dateToData["pH_hanna"] = []
    dateToData["orpMV_hanna"] = []
    dateToData["electricalConductivity_hanna"] = []
    dateToData["barometricPressure_hanna"] = []
    dateToData["dissolvedOxygenPercent_hanna"] = []
    dateToData["dissolvedOxygen_mgL_hanna"] = []

    for item in result:
        date = item[1]
        date = date.split(" ")[0]
        time = item[2]
        temperature = item[3]
        pH = item[4]
        orpMV = item[5]
        electricalConductivity = item[6]
        barometricPressure = item[7]
        dissolvedOxygenPercent = item[8]
        dissolvedOxygen_mgL = item[9]
        batch_id = item[29]

        # date = date.split(" ")[0]
        try:
            if date is None or time is None:
                continue
            datetime = date + " " + time
        except:
            print(date)
            print(time)
            print(traceback.format_exc())

        year, month, day, hour, minute, second = splitDatetimeQ(datetime)
        if int(year) >= 18: # ignore all the 2014 results
            index = datetimeToIndex(year, month, day, hour, minute, second)
            index = round(index / dayToIndexRatio) * dayToIndexRatio

            dateToData["batch_id"].append(batch_id)
            dateToData["datetime"].append(datetime)
            dateToData["index"].append(index)
            dateToData["temperature_hanna"].append(temperature)
            dateToData["pH_hanna"].append(pH)
            dateToData["orpMV_hanna"].append(orpMV)
            dateToData["electricalConductivity_hanna"].append(electricalConductivity)
            dateToData["barometricPressure_hanna"].append(barometricPressure)
            dateToData["dissolvedOxygenPercent_hanna"].append(dissolvedOxygenPercent)
            dateToData["dissolvedOxygen_mgL_hanna"].append(dissolvedOxygen_mgL)

    return dateToData

def getEureka(cursor, siteid):
    sqlquery = "SELECT *, MAX(eureka_batch_id) FROM (eureka_logs INNER JOIN eureka_batches USING(eureka_batch_id)) WHERE site_id = ? GROUP BY logging_date, logging_time, temp, ph_units, orp, sp_cond, turbidity, hdo_perc_sat, hdo_concentration, ph_mv, int_batt_v ORDER BY logging_date, logging_time;"
    sitetuple = (siteid,)
    cursor.execute(sqlquery, sitetuple)
    result = cursor.fetchall()
    dateToData = {}
    dateToData["batch_id"] = []
    dateToData["datetime"] = []
    dateToData["index"] = []
    dateToData["temperature_eureka"] = []
    dateToData["pH_eureka"] = []
    dateToData["orp_eureka"] = []
    dateToData["electricalConductivity_eureka"] = []
    dateToData["turbidity_eureka"] = []
    dateToData["hdo_percent_saturation_eureka"] = []
    dateToData["hdo_concentration_eureka"] = []
    dateToData["pH_mv_eureka"] = []

    for item in result:
        batch_id = item[0]
        ###
        date = item[0]
        time = item[1]
        temperature = item[2]
        pH = item[3]
        orp = item[4]
        electricalConductivity = item[5]
        turbidity = item[6]
        hdoPercentSaturation = item[7]
        hdoConcentration = item[8]
        phMV = item[9]
        # date = date.split(" ")[0]
        datetime = date + " " + time

        year, month, day, hour, minute, second = splitDatetimeP(datetime)
        index = datetimeToIndex(year, month, day, hour, minute, second)
        index = round(index / dayToIndexRatio) * dayToIndexRatio

        dateToData["batch_id"].append(batch_id)
        dateToData["datetime"].append(datetime)
        dateToData["index"].append(index)
        dateToData["temperature_eureka"].append(temperature)
        dateToData["pH_eureka"].append(pH)
        dateToData["orp_eureka"].append(orp)
        dateToData["electricalConductivity_eureka"].append(electricalConductivity)
        dateToData["turbidity_eureka"].append(turbidity)
        dateToData["hdo_percent_saturation_eureka"].append(hdoPercentSaturation)
        dateToData["hdo_concentration_eureka"].append(hdoConcentration)
        dateToData["pH_mv_eureka"].append(phMV)

    return dateToData

def getQ(cursor, siteid):
    sqlquery = "SELECT *, MAX(q_batch_id) FROM q_reads INNER JOIN q_batches USING (q_batch_id) where site_id = ? group by date_sampled, time_sampled order by (date_sampled);"
    sitetuple = (siteid,)
    cursor.execute(sqlquery, sitetuple)
    result = cursor.fetchall()
    dateToData = {}
    dateToData["batch_id"] = []
    dateToData["datetime"] = []
    dateToData["discharge_measured"] = []
    dateToData["index"] = []

    for item in result:
        batch_id = item[0]
        date = item[2]
        time = item[3]
        discharge = item[4]
        date = date.split(" ")[0]
        datetime = date + " " + time

        year, month, day, hour, minute, second = splitDatetimeQ(datetime)
        if int(year) >= 18: # protect against when the Hanna accidentally includes "2014" data
            index = datetimeToIndex(year, month, day, hour, minute, second)
            index = round(index / dayToIndexRatio) * dayToIndexRatio

            dateToData["batch_id"].append(batch_id)
            dateToData["datetime"].append(datetime)
            dateToData["discharge_measured"].append(discharge)
            dateToData["index"].append(index)

    return dateToData

def getElementar(cursor, siteid, nbsNum, citSciNum):

    elementar_non_average_view = "SELECT * FROM elementar_reads INNER JOIN (SELECT MAX(elementar_batch_id), * FROM elementar_batches GROUP BY file_name, date_run) USING (elementar_batch_id)"
    #elementarAverageView = "SELECT elementar_batch_id, hole, sort_chem, method, AVG(tic_area), AVG(tc_area), AVG(npoc_area), AVG(tnb_area), AVG(tic_mg_per_liter), AVG(tc_mg_per_liter), AVG(npoc_mg_per_liter), AVG(tnb_mg_per_liter), date_run, time_run, project_id, operator, file_name, file_path FROM (" + elementarNonAverageView + ") GROUP BY sort_chem, hole, elementar_batch_id"
    elementar_average_view = "SELECT *, MAX(elementar_batch_id) FROM (SELECT elementar_batch_id, hole, sort_chem, method, AVG(tic_area), AVG(tc_area), AVG(npoc_area), AVG(tnb_area), AVG(tic_mg_per_liter), AVG(tc_mg_per_liter), AVG(npoc_mg_per_liter), AVG(tnb_mg_per_liter), date_run, time_run, project_id, operator, file_name, file_path FROM(" + elementar_non_average_view  + ") GROUP BY sort_chem, elementar_batch_id) GROUP BY sort_chem ORDER BY sort_chem"
    sqlquery = "SELECT *, MAX(datetime_uploaded) FROM (" + elementar_average_view + ") JOIN sort_chems USING (sort_chem) WHERE site_id LIKE \"%NBS%" + nbsNum + "\" OR site_id LIKE \"%NBS%" + str(int(nbsNum)) + "\" OR site_id = ? OR site_id = ? GROUP BY sort_chem;"
    sitetuple = (siteid, citSciNum)
    cursor.execute(sqlquery, sitetuple)
    result = cursor.fetchall()

    dateToData = {}
    dateToData["batch_id"] = []
    dateToData["datetime"] = []
    dateToData["tic_mgl"] = []
    dateToData["tc_mgl"] = []
    dateToData["npoc_mgl"] = []
    dateToData["tnb_mgl"] = []
    dateToData["index"] = []

    for item in result:
        try:
            batch_id = item[0]
           ###
            tic_mgl = item[8]
            tc_mgl = item[9]
            npoc_mgl = item[10]
            tnb_mgl = item[11]
            date = str(item[21])
            time = str(item[22])

            datetime = getDatetime(date, time)
            year, month, day, hour, minute, second = splitDatetimeQ(datetime)
            index = datetimeToIndex(year, month, day, hour, minute, second)

            dateToData["batch_id"].append(batch_id)
            dateToData["datetime"].append(datetime)
            dateToData["tic_mgl"].append(tic_mgl)
            dateToData["tc_mgl"].append(tc_mgl)
            dateToData["npoc_mgl"].append(npoc_mgl)
            dateToData["tnb_mgl"].append(tnb_mgl)
            dateToData["index"].append(index)

        except:
            continue

    return dateToData


def getScanPar(cursor, siteid, nbsNum, citSciNum):


    #scanParIntermediate = "SELECT * FROM scan_par_reads INNER JOIN (SELECT * FROM scan_par_batches WHERE scan_par_batches.scan_par_batch_id = (SELECT MAX(scan_par_batch_id) FROM scan_datetimes_to_scan_par_batches GROUP BY datetime_run))"
    scanParIntermediate = "SELECT *, MAX(scan_par_batch_id) FROM scan_par_reads INNER JOIN scan_par_batches USING (scan_par_batch_id) GROUP BY datetime_run"
    scanParView = "SELECT * FROM (" + scanParIntermediate + ") INNER JOIN sort_chems_to_datetime_run USING (datetime_run)"
    sqlquery = "SELECT *, MAX(datetime_uploaded) FROM (" + scanParView + ") JOIN sort_chems USING (sort_chem) WHERE site_id LIKE \"%NBS%" + nbsNum + "\" OR site_id LIKE \"%NBS%" + str(int(nbsNum)) + "\" OR site_id = ? OR site_id = ? GROUP BY sort_chem;"
    sitetuple = (siteid, citSciNum)
    cursor.execute(sqlquery, sitetuple)
    result = cursor.fetchall()

    dateToData = {}
    dateToData["datetime"] = []
    dateToData["index"] = []
    dateToData["turbidity_scanPar"] = []
    dateToData["no3_scanPar"] = []
    dateToData["toc_scanPar"] = []
    dateToData["doc_scanPar"] = []

    for item in result:
        try:
            turbidity = item[2]
            no3 = item[3]
            toc = item[4]
            doc = item[5]
            date = str(item[15])
            time = str(item[16])

            datetime = getDatetime(date, time)
            year, month, day, hour, minute, second = splitDatetimeQ(datetime)
            index = datetimeToIndex(year, month, day, hour, minute, second)

            dateToData["datetime"].append(datetime)
            dateToData["index"].append(index)
            dateToData["turbidity_scanPar"].append(turbidity)
            dateToData["no3_scanPar"].append(no3)
            dateToData["toc_scanPar"].append(toc)
            dateToData["doc_scanPar"].append(doc)

        except:
            continue

    return dateToData

def getScanFp(cursor, siteid, nbsNum, citSciNum):

    #scanFPIntermediate = "SELECT * FROM scan_fp_reads INNER JOIN (SELECT * FROM scan_fp_batches WHERE scan_fp_batches.scan_fp_batch_id = (SELECT MAX(scan_fp_batch_id) FROM scan_datetimes_to_scan_fp_batches GROUP BY datetime_run))"
    scanFPIntermediate = "SELECT * FROM (SELECT *, MAX(scan_fp_batch_id) FROM scan_fp_reads INNER JOIN scan_fp_batches USING (scan_fp_batch_id) GROUP BY datetime_run) INNER JOIN sort_chems_to_datetime_run USING (datetime_run)"
    scanFPView = "SELECT * FROM (" + scanFPIntermediate + ") INNER JOIN sort_chems_to_datetime_run USING (datetime_run)"
    sqlquery = "SELECT *, MAX(datetime_uploaded) FROM (" + scanFPView + ") JOIN sort_chems USING (sort_chem) WHERE site_id LIKE \"%NBS%" + nbsNum + "\" OR site_id LIKE \"%NBS%" + str(int(nbsNum)) + "\" OR site_id = ? OR site_id = ? GROUP BY sort_chem;"
    sitetuple = (siteid, citSciNum)
    cursor.execute(sqlquery, sitetuple)
    result = cursor.fetchall()

    dateToData = {}
    dateToData["datetime"] = []
    dateToData["index"] = []

    startWavelength = 200
    endWavelength = 750
    # numWavelengths = ((endWavelength - startWavelength) // 4) + 1
    wavelengths = []
    wavelength = startWavelength
    prefix = "nm"
    i = 0
    while wavelength <= endWavelength:
        header = prefix + str(wavelength)
        wavelengths.append(header)
        dateToData[header] = []
        if i % 2 == 0:
            wavelength = wavelength + 2
        else:
            wavelength = wavelength + 3
        i = i + 1
    
    for item in result:
        try:

            date = str(item[232])
            time = str(item[233])

            datetime = getDatetime(date, time)
            year, month, day, hour, minute, second = splitDatetimeQ(datetime)
            index = datetimeToIndex(year, month, day, hour, minute, second)
        
            i = 2
            for wavelength in wavelengths:
                dateToData[wavelength].append(item[i])
                i = i + 1
            dateToData["datetime"].append(datetime)
            dateToData["index"].append(index)

        except:
            continue

    return dateToData



def getICCation(cursor, siteid, nbsNum, citSciNum):
    icCationView = "SELECT *, MAX(batch_id) FROM ic_cation_reads INNER JOIN ic_batches WHERE batch_id == ic_batch_id GROUP BY(sort_chem)"
    #icCationView = "SELECT * FROM ic_cation_reads INNER JOIN(SELECT * FROM ic_batches WHERE ic_batches.ic_batch_id = (SELECT MAX(ic_batch_id) FROM sort_chems_to_ic_cation_batches GROUP BY sort_chem)) WHERE batch_id = ic_batch_id"
    sqlquery = "SELECT *, MAX(datetime_uploaded) FROM (" + icCationView + ") JOIN sort_chems USING (sort_chem) WHERE site_id LIKE \"%NBS%" + nbsNum + "\" OR site_id LIKE \"%NBS%" + str(int(nbsNum)) + "\" OR site_id = ? OR site_id = ? GROUP BY sort_chem;"
    sitetuple = (siteid, citSciNum)
    cursor.execute(sqlquery, sitetuple)
    result = cursor.fetchall()

    dateToData = {}
    dateToData["datetime"] = []
    dateToData["index"] = []
    dateToData["lithium_ic"] = []
    dateToData["sodium_ic"] = []
    dateToData["ammonium_ic"] = []
    dateToData["potassium_ic"] = []
    dateToData["magnesium_ic"] = []
    dateToData["calcium_ic"] = []
    dateToData["strontium_ic"] = []

    for item in result:
        try:
            lithium = item[2]
            sodium = item[3]
            ammonium = item[4]
            potassium = item[5]
            magnesium = item[6]
            calcium = item[7]
            strontium = item[8]
            date = str(item[18])
            time = str(item[19])

            datetime = getDatetime(date, time)
            year, month, day, hour, minute, second = splitDatetimeQ(datetime)
            index = datetimeToIndex(year, month, day, hour, minute, second)

            dateToData["datetime"].append(datetime)
            dateToData["index"].append(index)
            dateToData["lithium_ic"].append(lithium)
            dateToData["sodium_ic"].append(sodium)
            dateToData["ammonium_ic"].append(ammonium)
            dateToData["potassium_ic"].append(potassium)
            dateToData["magnesium_ic"].append(magnesium)
            dateToData["calcium_ic"].append(calcium)
            dateToData["strontium_ic"].append(strontium)

        except:
            continue

    return dateToData

def getICAnion(cursor, siteid, nbsNum, citSciNum):
    icAnionView = "SELECT *, MAX(batch_id) FROM ic_anion_reads INNER JOIN ic_batches WHERE batch_id == ic_batch_id GROUP BY(sort_chem)"
    #icAnionView = "SELECT * FROM ic_anion_reads INNER JOIN(SELECT * FROM ic_batches WHERE ic_batches.ic_batch_id = (SELECT MAX(ic_batch_id) FROM sort_chems_to_ic_anion_batches GROUP BY sort_chem)) WHERE batch_id = ic_batch_id"
    sqlquery = "SELECT *, MAX(datetime_uploaded) FROM (" + icAnionView + ") JOIN sort_chems USING (sort_chem) WHERE site_id LIKE \"%NBS%" + nbsNum + "\" OR site_id LIKE \"%NBS%" + str(int(nbsNum)) + "\" OR site_id = ?  OR site_id = ? GROUP BY sort_chem;"
    sitetuple = (siteid,citSciNum)
    cursor.execute(sqlquery, sitetuple)
    result = cursor.fetchall()

    dateToData = {}
    dateToData["datetime"] = []
    dateToData["index"] = []
    dateToData["fluoride_ic"] = []
    dateToData["acetate_ic"] = []
    dateToData["formate_ic"] = []
    dateToData["chloride_ic"] = []
    dateToData["nitrite_ic"] = []
    dateToData["bromide_ic"] = []
    dateToData["nitrate_ic"] = []
    dateToData["sulfate_ic"] = []
    dateToData["phosphate_ic"] = []

    for item in result:
        try:
            fluoride = item[2]
            acetate = item[3]
            formate = item[4]
            chloride = item[5]
            nitrite = item[6]
            bromide = item[7]
            nitrate = item[8]
            sulfate = item[9]
            phosphate = item[10]
            date = str(item[20])
            time = str(item[21])

            datetime = getDatetime(date, time)
            year, month, day, hour, minute, second = splitDatetimeQ(datetime)
            index = datetimeToIndex(year, month, day, hour, minute, second)

            dateToData["datetime"].append(datetime)
            dateToData["index"].append(index)
            dateToData["fluoride_ic"].append(fluoride)
            dateToData["acetate_ic"].append(acetate)
            dateToData["formate_ic"].append(formate)
            dateToData["chloride_ic"].append(chloride)
            dateToData["nitrite_ic"].append(nitrite)
            dateToData["bromide_ic"].append(bromide)
            dateToData["nitrate_ic"].append(nitrate)
            dateToData["sulfate_ic"].append(sulfate)
            dateToData["phosphate_ic"].append(phosphate)
        except:
            continue

    return dateToData

def getICP(cursor, siteid, nbsNum, citSciNum):
    icpView = "SELECT *, MAX(icp_batch_id) FROM icp_reads_1 INNER JOIN icp_batches_1 USING(icp_batch_id) GROUP BY(sort_chem)"
    #icpView = "SELECT * FROM icp_reads_1 INNER JOIN(SELECT * FROM icp_batches_1 WHERE icp_batches_1.icp_batch_id = (SELECT MAX(icp_batch_id) FROM sort_chems_to_icp_batches_1 GROUP BY sort_chem)) USING(icp_batch_id)"
    sqlquery = "SELECT *, MAX(datetime_uploaded) FROM (" + icpView + ") JOIN sort_chems USING (sort_chem) WHERE site_id LIKE \"%NBS%" + nbsNum + "\" OR site_id LIKE \"%NBS%" + str(int(nbsNum)) + "\" OR site_id = ? OR site_id = ? GROUP BY sort_chem;"
    sitetuple = (siteid,citSciNum)
    cursor.execute(sqlquery, sitetuple)
    result = cursor.fetchall()

    dateToData = {}
    dateToData["datetime"] = []
    dateToData["index"] = []
    dateToData["aluminum_icp"] = []
    dateToData["arsenic_icp"] = []
    dateToData["boron_icp"] = []
    dateToData["barium_icp"] = []
    dateToData["calcium_icp"] = []
    dateToData["cadmium_icp"] = []
    dateToData["cobalt_icp"] = []
    dateToData["chromium_icp"] = []
    dateToData["copper_icp"] = []
    dateToData["iron_icp"] = []
    dateToData["potassium_icp"] = []
    dateToData["magnesium_icp"] = []
    dateToData["manganese_icp"] = []
    dateToData["molybdenum_icp"] = []
    dateToData["sodium_icp"] = []
    dateToData["nickel_icp"] = []
    dateToData["phosphorus_icp"] = []
    dateToData["lead_icp"] = []
    dateToData["sulfur_icp"] = []
    dateToData["selenium_icp"] = []
    dateToData["silicon_icp"] = []
    dateToData["strontium_icp"] = []
    dateToData["titanium_icp"] = []
    dateToData["vanadium_icp"] = []
    dateToData["zinc_icp"] = []

    for item in result:
        try:

            aluminum = item[2]
            arsenic = item[3]
            boron = item[4]
            barium = item[5]
            calcium = item[6]
            cadmium = item[7]
            cobalt = item[8]
            chromium = item[9]
            copper = item[10]
            iron = item[11]
            potassium = item[12]
            magnesium = item[13]
            manganese = item[14]
            molybdenum = item[15]
            sodium = item[16]
            nickel = item[17]
            phosphorus = item[18]
            lead = item[19]

            sulfur = item[20]
            selenium = item[21]
            silicon = item[22]
            strontium = item[23]
            titanium = item[24]
            vanadium = item[25]
            zinc = item[26]

            date = str(item[34])
            time = str(item[35])

            datetime = getDatetime(date, time)
            year, month, day, hour, minute, second = splitDatetimeQ(datetime)
            index = datetimeToIndex(year, month, day, hour, minute, second)

            dateToData["datetime"].append(datetime)
            dateToData["index"].append(index)
            dateToData["aluminum_icp"].append(aluminum)
            dateToData["arsenic_icp"].append(arsenic)
            dateToData["boron_icp"].append(boron)
            dateToData["barium_icp"].append(barium)
            dateToData["calcium_icp"].append(calcium)
            dateToData["cadmium_icp"].append(cadmium)
            dateToData["cobalt_icp"].append(cobalt)
            dateToData["chromium_icp"].append(chromium)
            dateToData["copper_icp"].append(copper)
            dateToData["iron_icp"].append(iron)
            dateToData["potassium_icp"].append(potassium)
            dateToData["magnesium_icp"].append(magnesium)
            dateToData["manganese_icp"].append(manganese)
            dateToData["molybdenum_icp"].append(molybdenum)
            dateToData["sodium_icp"].append(sodium)
            dateToData["nickel_icp"].append(nickel)
            dateToData["phosphorus_icp"].append(phosphorus)
            dateToData["lead_icp"].append(lead)
            dateToData["sulfur_icp"].append(sulfur)
            dateToData["selenium_icp"].append(selenium)
            dateToData["silicon_icp"].append(silicon)
            dateToData["strontium_icp"].append(strontium)
            dateToData["titanium_icp"].append(titanium)
            dateToData["vanadium_icp"].append(vanadium)
            dateToData["zinc_icp"].append(zinc)

        except:
            continue
    
    return dateToData


def getFieldSheetInfo(cursor, siteid, nbsNum, citSciNum):
    sqlquery = "SELECT *, MAX(datetime_uploaded) FROM sort_chems WHERE date_sampled != \"NULL\" AND date_sampled != \"None\" AND time_sampled != \"NULL\" AND time_sampled != \"None\" AND site_id LIKE \"%NBS." + nbsNum + "\" OR site_id LIKE \"%NBS " + nbsNum + "\" OR site_id LIKE \"%NBS" + nbsNum + "\" OR site_id LIKE \"%NBS." + str(int(nbsNum)) + "\" OR site_id LIKE \"%NBS " + str(int(nbsNum)) + "\" OR site_id LIKE \"%NBS" + str(int(nbsNum)) + "\" OR site_id = ? OR site_id = ? GROUP BY sort_chem;"
    sitetuple = (siteid, citSciNum)
    cursor.execute(sqlquery, sitetuple)
    result = cursor.fetchall()

    dateToData = {}
    dateToData["datetime"] = []
    dateToData["index"] = []
    dateToData["temperature_fieldsheet"] = []
    dateToData["pH_fieldsheet"] = []
    dateToData["orpMV_fieldsheet"] = []
    dateToData["electricalConductivity_fieldsheet"] = []
    dateToData["barometricPressure_fieldsheet"] = []
    dateToData["dissolvedOxygenPercent_fieldsheet"] = []
    dateToData["dissolvedOxygen_mgL_fieldsheet"] = []
    dateToData["device"] = []
    dateToData["chlorophyl_ugl"] = []
    dateToData["chlorophyl_rfu"] = []
    dateToData["pc_ug"] = []
    dateToData["calibrated_fieldsheet"] = []
    dateToData["qGrams_fieldsheet"] = []

    for item in result:
        try:

            ph = item[5]
            orp = item[6]
            o2percent = item[7]
            o2mg = item[8]
            conductance = item[9]
            temperature = item[10]
            pressure = item[11]
            calibrated = item[12]
            qGrams = item[13]
            device = item[-5]
            chlorophyl_ugl = item[-4]
            chlorophyl_rfu = item[-3]
            pc_ug = item[-2]
            
            if calibrated != None:
                calibrated = str(calibrated)
                calibrated = calibrated.lower()
                if "yes" in calibrated:
                    calibrated = 1
                else:
                    calibrated = None

            date = str(item[2])
            time = str(item[3])
            datetime = getDatetime(date, time)
            year, month, day, hour, minute, second = splitDatetimeQ(datetime)
            index = datetimeToIndex(year, month, day, hour, minute, second)

            dateToData["datetime"].append(datetime)
            dateToData["index"].append(index)
            dateToData["temperature_fieldsheet"].append(temperature)
            dateToData["pH_fieldsheet"].append(ph)
            dateToData["orpMV_fieldsheet"].append(orp)
            dateToData["electricalConductivity_fieldsheet"].append(conductance)
            dateToData["barometricPressure_fieldsheet"].append(pressure)
            dateToData["dissolvedOxygenPercent_fieldsheet"].append(o2percent)
            dateToData["dissolvedOxygen_mgL_fieldsheet"].append(o2mg)
            dateToData["calibrated_fieldsheet"].append(calibrated)
            dateToData["qGrams_fieldsheet"].append(qGrams)
            dateToData["device"].append(device)
            dateToData["chlorophyl_ugl"].append(chlorophyl_ugl)
            dateToData["chlorophyl_rfu"].append(chlorophyl_rfu)
            dateToData["pc_ug"].append(pc_ug)
        except:
            continue

    return dateToData
