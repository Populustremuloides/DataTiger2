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
        if (len(rowList) != 12) and (len(rowList) != 11):
            raise EurekaFileIncorrectlyFormated(self.filePath)

        self.loggingDate = re.sub("/", "-", rowList[0])
        self.loggingTime = rowList[1]
        self.temp = rowList[2]
        self.phUnits = rowList[3]
        self.orp = rowList[4]
        self.spCond = rowList[5]
        self.turbidity = rowList[6]
        self.hdoPercSat = rowList[7]
        self.hdoConcentration = rowList[8]
        self.phMv = rowList[9]
        self.intBattV = rowList[10]
        # the last element is blank

