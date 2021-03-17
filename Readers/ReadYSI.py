from datetime import datetime
from Readers.ReadHanna import *
import pandas as pd

class ReadYSI:
    def __init__(self, filePath):
        self.filePath = filePath

        cleanPath = self.filePath.replace("\\", "/")
        cleanPath = cleanPath.replace("//", "/")
        pathList = cleanPath.split("/")
        self.fileName = pathList[-1]

        self.noErrors = 0
        self.missingValues = -1
        self.invalidRemarks = -3
        self.missingHeaders = -2

        self.datetimeUploaded = None

        self.date = None
        self.time = None
        self.ec = None

        self.dateIndex = None
        self.timeIndex = None
        self.ecIndex = None


    def batchMissingHeaders(self):
        if self.dateIndex == None:
            return True
        if self.timeIndex == None:
            return True
        if self.ecIndex == None:
            return True

        return False

    def resetValues(self):
        self.date = None
        self.time = None
        self.ec = None


    def fixDate(self, date):
        month, day, year = date.split("/")

        month = int(month)
        day = int(day)
        year = int(year)

        if month > 2000:
            nyear = month
            nmonth = day
            nday = year

            day = str(nday)
            month = str(nmonth)
            year = str(nyear)

        month = str(month)
        day = str(day)
        year = str(year)

        return year + '-' + month + "-" + day

    def getValues(self, row):
        if self.timeIndex != None:
            self.time = str(row[self.timeIndex])
        if self.dateIndex != None:
            self.date = self.fixDate(str(row[self.dateIndex]))
        if self.ecIndex != None:
            self.ec = float(row[self.ecIndex])

    def replaceNan(self, value):
        if pd.isna(value):
            return None
        else:
            return value

    def cleanValues(self):
        self.date = self.replaceNan(self.date)
        self.time = self.replaceNan(self.time)
        self.ec = self.replaceNan(self.ec)

    def rowMissingValues(self):
        if self.date == None:
            return True
        if self.time == None:
            return True
        if self.ec == None:
            return True

        return False


    def readRow(self, row):

        # get the data
        self.resetValues()
        self.getValues(row)
        self.cleanValues()

        # make sure there are no values missing
        if self.rowMissingValues():
            return self.missingValues

        return self.noErrors

    def readBatch(self, headers):
        # get the datetime uploaded
        self.datetimeUploaded = str(datetime.now())

        # register the indices of the date/time and conductivity
        i = 0
        for header in headers:
            header = header.lower()
            if "date" in header:
                self.dateIndex = i
            elif "time" in header:
                self.timeIndex = i
            elif "spc" in header:
                self.ecIndex = i

            i = i + 1

        if self.batchMissingHeaders():
            return self.missingHeaders
        else:
            return self.noErrors
