from CustomErrors import *
import pandas as pd
import re
from math import isnan
from datetime import datetime
from UnitConversions import *

class ReadHanna():
    def __init__(self, path):

        # for the info sheet
        self.instrumentName = None
        self.instrumentId = None
        self.serialNum = None
        self.pcSoftwareVersion = None
        self.meterSoftwareVersion = None
        self.meterSoftwareDate = None
        self.referenceTemp = None
        self.temperatureCoefficient = None
        self.tdsFactor = None
        self.lotName = None
        self.remarks = None
        self.startDate = None
        self.startTime = None
        self.samplesNo = None
        self.loggingInterval = None
        self.numParameters = None

        self.pressureHeader = None

        # for the data sheet
        self.date = None
        self.time = None
        self.temp = None
        self.pH = None
        self.orp = None
        self.ec = None
        self.pressure = None
        self.dissolvedOxygenPercent = None
        self.dissolvedOxygen = None
        self.remarks = None

        self.pressureConversionATM = False
        self.pressureConversionKPA = False
        self.pressureConversionPSI = False
        self.pressureConversionIdentity = False

        # Grab the first three letters of the file name
        self.filePath = path
        cleanPath = path.replace("\\", "/")
        cleanPath = cleanPath.replace("//", "/")
        pathList = cleanPath.split("/")
        self.fileName = pathList[-1]
        self.sitePrefix = self.fileName[0:3].upper()

        self.dateIndex = None
        self.timeIndex = None
        self.tempIndex = None
        self.pHIndex = None
        self.orpIndex = None
        self.ecIndex = None
        self.pressureIndex = None
        self.dissolvedOxygenPercentIndex = None
        self.dissolvedOxygenIndex = None
        self.remarksIndex = None

        self.dataSheetHeadersCalled = False

    def getFilePath(self):
        return self.filePath

    def getFileName(self):
        return self.fileName

    def readInfoSheet(self, df):
        try:
            for index, row in df.iterrows():
                #if index == 1:
                    if row[0] == "Instrument Name":
                        self.instrumentName = row[1]
                    # else:
                    #     print("tihs is the row:")
                    #     print(row)
                    #     raise hannaInfoSheetChanged(0)
                    #     # TODO: change the errors to be specific
                #if index == 1:
                    if row[0] == "Instrument ID":
                        if pd.isnull(row[1]):
                            self.instrumentId = "NULL"
                            print("null value detected")
                        else:
                            self.instrumentId = row[1]
                    # else:
                    #     raise hannaInfoSheetChanged(0)
                # if index == 2:
                    if row[0] == "Instrument Serial No.":
                        self.serialNum = row[1]
                    # else:
                    #     raise hannaInfoSheetChanged(2)
                #if index == 3:
                    if row[0] == "PC Software Version":
                        self.pcSoftwareVersion = row[1]
                    # else:
                    #     raise hannaInfoSheetChanged(3)
                #if index == 4:
                    if row[0] == "Meter Software Version":
                        self.meterSoftwareVersion = row[1]
                    # else:
                    #     raise hannaInfoSheetChanged(4)
                #if index == 5:
                    if row[0] == "Meter Software Date":
                        self.meterSoftwareDate = row[1]
                    # else:
                    #     raise hannaInfoSheetChanged(5)
                #if index == 9:
                    if row[0] == "Reference Temperature":
                        if row[1].endswith("C"): # make sure it's celcius
                            # save just the number
                            nums = [int(s) for s in row[1].split() if s.isdigit()]
                            self.referenceTemp = nums[0]
                        # else:
                        #     raise hannaInfoSheetChanged(9)
                    # else:
                    #     raise hannaInfoSheetChanged(9)
                #if index == 10:
                    if row[0] == "Temperature Coefficient":
                        if row[1].endswith("C"): # make sure it's celcius
                            num = (row[1].split(" "))[0]
                            self.temperatureCoefficient = num
                        # else:
                        #     raise hannaInfoSheetChanged(10)
                    # else:
                    #     raise hannaInfoSheetChanged(10)
                #if index == 11:
                    if row[0] == "TDS Factor":
                        self.tdsFactor = row[1]
                    # else:
                    #     raise hannaInfoSheetChanged(11)
                #if index == 15:
                    if row[0] == "Lot Name":
                        if len(row[1]) >= 3:
                            self.lotName = row[1][0:3] # just save the 3 letters representing the site
                        else:
                            self.lotName = str(row[1])
                    # else:
                    #     raise hannaInfoSheetChanged(15)

                #if index == 16:
                    if row[0] == "Remarks":
                        self.remarks = row[1]
                    # else:
                    #     raise hannaInfoSheetChanged(16)
                #if index == 17:
                    if row[0] == "Started Date and Time":
                        text = row[1].split(" - ")

                        # remove extra space
                        self.startDate = text[0]
                        self.startTime = text[1]

                        # format the strings for sqlite
                        self.startDate = re.sub("\W+", "-", self.startDate)
                        self.startTime = re.sub("\W+", ":", self.startTime)

                    # else:
                    #     raise hannaInfoSheetChanged(17)

                #if index == 18:
                    if row[0] == "Samples No":
                        self.samplesNo = row[1]
                    # else:
                    #     raise hannaInfoSheetChanged(18)
                #if index == 19:
                    if row[0] == "Logging Interval":
                        if row[1] == "---":
                            self.loggingInterval = None
                        else:
                            self.loggingInterval = row[1]
                    # else:
                    #     raise hannaInfoSheetChanged(19)
                #if index == 20:
                    if row[0] == "Parameters No.":
                        self.numParameters = row[1]
                    # else:
                    #     raise hannaInfoSheetChanged(20)

            #if self.instrumentId == "Nan":
            #    print("got an nan")

            # print(self.instrumentName)
            # print(self.serialNum)
            # print(self.pcSoftwareVersion)
            # print(self.meterSoftwareVersion)
            # print(self.meterSoftwareDate)
            # print(self.referenceTemp)
            # print(self.temperatureCoefficient)
            # print(self.tdsFactor)
            # print(self.lotName)
            # print(self.remarks)
            # print(self.startDate)
            # print(self.startTime)
            # print(self.samplesNo)
            # print(self.loggingInterval)
            # print(self.numParameters)

            return [0]

        except hannaInfoSheetChanged as e:
            return [1, e] # fixme: this might cause problems

    def readDataSheetHeaders(self, headers):
        i = 0
        for header in headers:
            header = header.lower()
            if "date" in header:
                self.dateIndex = i
            elif "time" in header:
                self.timeIndex = i
            elif "temp" in header:
                self.tempIndex = i
            elif "ph" in header and "mv" not in header:
                self.pHIndex = i
            elif "orp" in header:
                self.orpIndex = i
            elif "ec" in header:
                self.ecIndex = i
            elif "press" in header:
                self.pressureIndex = i
                if "mmhg" in header:
                    self.pressureConversionATM = False
                    self.pressureConversionKPA = False
                    self.pressureConversionPSI = False
                    self.pressureConversionIdentity = True

                elif "kpa" in header:
                    print("converting from kpa to mmHg")
                    self.pressureConversionATM = False
                    self.pressureConversionKPA = True
                    self.pressureConversionPSI = False
                    self.pressureConversionIdentity = False

                elif "atm" in header:
                    print("converting from ATM to mmHg")
                    self.pressureConversionATM = True
                    self.pressureConversionKPA = False
                    self.pressureConversionPSI = False
                    self.pressureConversionIdentity = False
                elif "psi" in header:
                    print("converting from PSI to mmHg")
                    self.pressureConversionATM = False
                    self.pressureConversionKPA = False
                    self.pressureConversionPSI = True
                    self.pressureConversionIdentity = False
                else:
                    raise hannaPressureUnitNotRecognized(header)
            elif "d.o." in header and "%" in header:
                self.dissolvedOxygenPercentIndex = i
            elif "d.o." in header and "mg" in header:
                self.dissolvedOxygenIndex = i
            elif "remarks" in header:
                self.remarksIndex = i
            i = i + 1
        self.dataSheetHeadersCalled = True

    def resetValues(self):
        self.date = None
        self.time = None
        self.temp = None
        self.pH = None
        self.orp = None
        self.ec = None
        self.pressure = None
        self.dissolvedOxygenPercent = None
        self.dissolvedOxygen = None
        self.remarks = None

    def assignRowValues(self, row):
        if self.dateIndex != None:
            if "--" not in str(row[self.dateIndex]):
                if type(row[self.dateIndex]) == type(""):
                    self.date = row[self.dateIndex]
                    self.date = self.date.replace("/","-")
                else:
                    self.date = (row[self.dateIndex]).strftime("%Y-%m-%d")
        if self.timeIndex != None:
            if "--" not in str(row[self.timeIndex]):
                if type(row[self.dateIndex]) == type(""):
                    self.time = row[self.timeIndex]
                else:
                    self.time = (row[self.timeIndex]).strftime("%H:%M:%S")
        if self.tempIndex != None:
            if "--" not in str(row[self.tempIndex]):
                self.temp = row[self.tempIndex]
        if self.pHIndex != None:
            if "--" not in str(row[self.pHIndex]):
                self.pH = row[self.pHIndex]
        if self.orpIndex != None:
            if "--" not in str(row[self.orpIndex]):
                self.orp = row[self.orpIndex]
        if self.ecIndex != None:
            if "--" not in str(row[self.ecIndex]):
                self.ec = float(row[self.ecIndex])
        if self.pressureIndex != None:
            if "--" not in str(row[self.pressureIndex]):
                self.pressure = row[self.pressureIndex]
                if self.pressureConversionATM:
                    self.pressure = atmTommHg(self.pressure)
                elif self.pressureConversionKPA:
                    self.pressure = kpaTommHg(self.pressure)
                elif self.pressureConversionPSI:
                    self.pressure = psiTommHg(self.pressure)
                elif self.pressureConversionIdentity:
                    self.pressure = self.pressure
                else:

                    raise UnrecognizablePressureUnit()
        if self.dissolvedOxygenPercentIndex != None:
            if "--" not in str(row[self.dissolvedOxygenPercentIndex]):
                self.dissolvedOxygenPercent = row[self.dissolvedOxygenPercentIndex]
        if self.dissolvedOxygenIndex != None:
            if "--" not in str(row[self.dissolvedOxygenIndex]):
                self.dissolvedOxygen = row[self.dissolvedOxygenIndex]
        if self.remarksIndex != None:
            if "--" not in str(row[self.remarksIndex]):
                self.remarks = row[self.remarksIndex]

    def cleanRowValues(self):
        if pd.isna(self.date) or "--" in str(self.date):
            self.date = None
        if pd.isna(self.time) or "--" in str(self.time):
            self.time = None
        if pd.isna(self.temp) or "--" in str(self.temp):
            self.temp = None
        if pd.isna(self.pH) or "--" in str(self.pH):
            self.pH = None
        if pd.isna(self.orp) or "--" in str(self.orp):
            self.orp = None
        if pd.isna(self.ec) or "--" in str(self.ec):
            self.ec = None
        if pd.isna(self.pressure) or "--" in str(self.pressure):
            self.pressur = None
        if pd.isna(self.dissolvedOxygenPercent) or "--" in str(self.dissolvedOxygenPercent):
            self.dissolvedOxygenPercent = None
        if pd.isna(self.dissolvedOxygen) or "--" in str(self.dissolvedOxygen):
            self.dissolvedOxygen = None
        if pd.isna(self.remarks) or "--" in str(self.remarks):
            self.remarks = None

    def readDataSheetRow(self, df, rowIndex):
        if not self.dataSheetHeadersCalled:
            self.readDataSheetHeaders(list(df.columns.values))
        try:

            self.resetValues()
            row = list(df.loc[rowIndex]) #convert from series object to list

            # if len(row) < 5:
            #     return [1, errorProcessingHannaData]

            if not isnan(row[5]): # check to see if this is the bottom of the table

                self.assignRowValues(row)
                self.cleanRowValues()
                return [0]

        except errorProcessingHannaData as e:
            print("error in ReadHanna")
            return [1, e]
#

