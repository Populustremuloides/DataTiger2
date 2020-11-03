import pandas as pd
from CustomErrors import EurekaInfoNotFound, EurekaFileIncorrectlyNamed, EurekaFileIncorrectlyFormated
import csv
import re

class ReadEureka():
    def __init__(self, filePath):
        self.filePath = filePath
        self.name = None
        self.make = None
        self.serialNum = None

        cleanPath = self.filePath.replace("\\", "/")
        cleanPath = cleanPath.replace("//", "/")
        pathList = cleanPath.split("/")
        self.fileName = pathList[-1]

        self.loggingDateIndex = None
        self.loggingTimeIndex = None
        self.tempIndex = None
        self.phIndex = None
        self.orpIndex = None
        self.conductivityIndex = None
        self.turbidityIndex = None
        self.hdoPercSatIndex = None
        self.hdoConcentrationIndex = None
        self.phMVIndex = None
        self.intBattVIndex = None


    def getPath(self):
        return self.filePath

    def getFileName(self):
        return self.fileName

    def readEurekaInfo(self):
        # name, make, serialNum are all contained in the second row (index 1)
        with open(self.filePath) as csvFile:
            reader = csv.reader(csvFile, delimiter=",")
            headerNotFound = True
            while headerNotFound:
                try:
                    row = next(reader)
                    if "ureka" in row[0]:
                        headerNotFound = False
                        self.name = row[0]
                        self.make = row[1]
                        self.serialNum = row[2]
                        return
                except:
                    raise EurekaInfoNotFound(self.filePath)

    def readBatchInfo(self):
        with open(self.filePath) as csvFile: # try to extract it from the file
            reader = csv.reader(csvFile, delimiter=",")
            self.siteId = self.fileName[0:3].upper()
            if not (self.siteId.isalpha()): # if you can't extract the site from the name
                try:
                    row = next(reader)
                    self.siteId = (row[0]).split(".")[0].upper() # by looking at the first 3 letters of the first row
                except:
                    raise EurekaInfoNotFound(self.filePath)

            # get first reading time and date
            dateNotFound = True
            while dateNotFound:
                try:
                    row = list(next(reader))
                    if re.match(r"^\d+/\d+/\d+", row[0]): # keep only those rows that start with a date
                        dateNotFound = False
                        self.firstLoggingDate = re.sub("/", "-", row[0])
                        self.firstLoggingTime = row[1]
                except:
                    raise EurekaInfoNotFound(self.filePath)
        with open(self.filePath) as csvFile:  # try to extract it from the file
            newReader = csv.reader(csvFile, delimiter=",")
            headerLine = False
            for line in newReader:
                # line = line.split(",")
                print(line)
                print("foo")
                for element in line:
                    if "date" in element.lower() or "temp" in element.lower() or "time" in element.lower():
                        headerLine = True

                if headerLine:
                    indx = 0
                    for element in line:
                        element = element.lower()
                        if "date" in element:
                            self.loggingDateIndex = indx
                        if "time" in element:
                            self.loggingTimeIndex = indx
                        if "temp" in element:
                            self.tempIndex = indx
                        if "ph" in element and "units" in element:
                            self.phIndex = indx
                        if "orp" in element and "mv" in element:
                            self.orpIndex = indx
                        if "cond" in element:
                            self.conductivityIndex = indx
                        if "turb" in element:
                            self.turbidityIndex = indx
                        if "hdo" in element and "sat" in element:
                            self.hdoPercSatIndex = indx
                        if "hdo" in element and "mg" in element:
                            self.hdoConcentrationIndex = indx
                        if "ph" in element and "mv" in element:
                            self.phMVIndex = indx
                        if "batt" in element:
                            self.intBattVIndex = indx
                        indx = indx + 1

                    break

        if not self.siteId.isalpha(): # otherwise raise an error
            raise EurekaFileIncorrectlyNamed(self.filePath)

        # get date extracted

        rawDateExtracted = self.fileName.split(".")[0][3:9] # FIXME: this isn't right
        if (not rawDateExtracted.isnumeric()): # if you cannot extract it from the file name
            raise EurekaFileIncorrectlyNamed(self.filePath) # extract it from the

        year = rawDateExtracted[0:2]
        month = rawDateExtracted[2:4]
        day = rawDateExtracted[4:]

        self.dateExtracted = "20" + year + "-" + month + "-" + day
        self.siteId = self.siteId.upper()

    def readRow(self, rowList):
        # if (len(rowList) != 12) and (len(rowList) != 11):
        #     raise EurekaFileIncorrectlyFormated(self.filePath)

        if len(rowList) > self.loggingDateIndex:
            self.loggingDate = re.sub("/", "-", rowList[self.loggingDateIndex])
        else:
            self.loggingDate = None

        if len(rowList) > self.loggingTimeIndex:
            self.loggingTime = rowList[self.loggingTimeIndex]
        else:
            self.loggingTime = None

        if len(rowList) > self.tempIndex:
            self.temp = rowList[self.tempIndex]
        else:
            self.temp = ""

        if len(rowList) > self.phIndex:
            self.phUnits = rowList[self.phIndex]
        else:
            self.phUnits = ""

        if len(rowList) > self.orpIndex:
            self.orp = rowList[self.orpIndex]
        else:
            self.orp = ""

        if len(rowList) > self.conductivityIndex:
            self.spCond = rowList[self.conductivityIndex]
        else:
            self.spCond = ""

        if len(rowList) > self.turbidityIndex:
            self.turbidity = rowList[self.turbidityIndex]
        else:
            self.turbidity = ""

        if len(rowList) > self.hdoPercSatIndex:
            self.hdoPercSat = rowList[self.hdoPercSatIndex]
        else:
            self.hdoPercSat = ""

        if len(rowList) > self.hdoConcentrationIndex:
            self.hdoConcentration = rowList[self.hdoConcentrationIndex]
        else:
            self.hdoConcentration = ""

        if len(rowList) > self.phMVIndex:
            self.phMv = rowList[self.phMVIndex]
        else:
            self.phMv = ""

        if len(rowList) > self.intBattVIndex:
            self.intBattV = rowList[self.intBattVIndex]
        else:
            self.intBattV = ""

        # the last element is blank
        # print(self.loggingDate)
        # print(self.loggingTime)
        # print(self.temp)
        # print(self.phUnits)
        # print(self.orp)
        # print(self.spCond)
        # print(self.turbidity)
        # print(self.hdoPercSat)
        # print(self.hdoConcentration)
        # print(self.phMv)
        # print(self.intBattV)
        #
        # if (self.phUnits.isalpha()):
        #     input("i rest my case") # somehow this never gets triggered

