import traceback
import pandas as pd
import requests
import datetime
import numpy as np
import re
import os
from USGS_downloaders.date_to_index import *
from USGS_downloaders.calculate_dist import get_closest_usgs_sites

def getLineData(inputRow, siteIndex, dateIndex, flowIndex, qualityIndex):
    site = inputRow[siteIndex]
    date = inputRow[dateIndex]
    flow = inputRow[flowIndex]
    quality = inputRow[qualityIndex]
    return site, date, flow, quality

def linkDataToString(link, catchment, startYear, days):
    data = []

    # get the data from the current link
    f = requests.get(link)
    inputRows = f.text.split("\n")

    if len(inputRows) < 10: # if the link was a dud
        return data

    # scan to make sure it contains the right kind of data
    numLinesCopied = 1
    numGoodLines = 0

    sites = []
    lineDates = []
    flows = []
    qualities = []
    indices = []

    for i in range(len(inputRows)):

        inputRow = inputRows[i]

        if not inputRow.startswith("#"): # only copy lines with actual data
            combinedLine = inputRow
            inputRow = inputRow.split("\t")

            # grab the indices of the various data columns
            if numGoodLines == 0:
                if "_00060_" not in combinedLine: # if it doesn't contain the right kind of data
                    return data

                siteIndex, dateIndex, flowIndex, qualityIndex = getHeaderIndices(inputRow)
                numGoodLines += 1

            elif numGoodLines == 1:
                # skip the junk line
                numGoodLines += 1

            else:

                # get the data for that row
                if len(inputRow) > 1:

                    # extract the current information from here
                    site, lineDate, flow, quality = getLineData(inputRow, siteIndex, dateIndex, flowIndex, qualityIndex)
                    index = usgs_date_to_index(lineDate, startYear)

                    sites.append(site)
                    lineDates.append(lineDate)
                    flows.append(flow)
                    qualities.append(quality)
                    indices.append(index)

                    if int(numLinesCopied) < index: # if there is a gap

                        while int(numLinesCopied) < index:
                            data.append("") # append a new comma followed by blank data
                            # else: append blankdata
                            numLinesCopied += 1

                        # now that we're caught up, write down the new data
                        if "A" in quality:
                            data.append(str(flow))
                        else:
                            data.append("")
                            # else: append blank data

                        numLinesCopied += 1

                    elif int(numLinesCopied) == int(index): # if the two line up
                        if "A" in quality:
                            data.append(str(flow))
                        else:
                            data.append("")
                            # else: append blank data
                        numLinesCopied += 1

                    else:
                        print(lineDate)
                        print(numLinesCopied)
                        print(index)
                        print("THERE WAS A MISTAKE")
                    numGoodLines += 1

    while len(data) <= days:
        data.append("")
        numLinesCopied += 1

    d = {'sites': sites, 'date': lineDates, 'flows': flows, 'indices': indices, 'qualities': qualities}
    df = pd.DataFrame.from_dict(d)

    return data, df

def getHeaderIndices(headers):
    j = 0
    for header in headers:
        if "site_no" in header:
            siteIdIndex = j
        elif "datetime" in header:
            dateIndex = j
        elif "_00060_" in header and "_cd" not in header: # seems to always be reported in cubic feet per second
            flowIndex = j
        elif "_00060_" in header and "_cd" in header:
            qualityIndex = j
        j = j + 1
    return siteIdIndex, dateIndex, flowIndex, qualityIndex

def get_usgs_discharge_sites():
    startYear = 2018
    kilometerToMileRatio = 0.621371

    link1 = "https://waterservices.usgs.gov/nwis/dv/?format=rdb&sites="
    link2 = "&period=P"

    today = datetime.datetime.today()
    today = str(today)
    date, time = today.split(" ")
    currentIndex = usgs_date_to_index(date, startYear) - 1
    days = currentIndex
    link3 = "D"
    catchment_dfs = {}

    corr_catchments = pd.read_csv("USGS_downloaders/usgs_catchment_characteristics.csv")


    catchments, sites_dict, sites_dict_verbose = get_closest_usgs_sites(corr_catchments)

    links = []
    for catchment in catchments:
        link = link1 + str(catchment) + link2 + str(days) + link3
        links.append(link)

    for j in range(len(links)):
        link = links[j]
        catchment = catchments[j]
        try:
            rowData, df = linkDataToString(link, catchment, startYear, days)
        except:
            print(f"catchment {catchment} did not return any valid data")
            continue

        sites = list(set(df.sites.values.tolist()))
        if len(sites) != 1:
            print("ERROR")
            site = 'err'
        else:
            site = sites[0]

        df['flows'] = pd.to_numeric(df['flows'], errors='coerce')
        df['flows'] = pd.to_numeric(df['flows'], errors='coerce')
        df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d', errors='ignore')
        
        catchment_dfs[site] = df

    sites_dict = find_valid_sites(catchment_dfs, sites_dict_verbose)

    return catchment_dfs, sites_dict

def find_valid_sites(catchment_dfs, sites_dict_v):
    sites_dict = {}
    for site, usgs_site_array in sites_dict_v.items():
        for usgs_site in usgs_site_array:
            if usgs_site in catchment_dfs.keys():
                sites_dict[site] = usgs_site

                # some of the usgs_sites didn't return valid data, in that case skip to the next one

            # # We only care to look at USGS sites whose data are inclusive of the period from 2018 to the present
            # if latest_date >= datetime.date(2018, 1, 1):

    if site not in sites_dict.keys():
        print('ERR: the three closest USGS sites didn\'t provide any meaningful data')
        sites_dict[site] = sites_dict_v[site][0]

    return sites_dict
