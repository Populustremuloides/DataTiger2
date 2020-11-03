from datetime import datetime
from Readers.ReadHanna import *
import pandas as pd

class ReadQ:
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
        self.remarks = None

        self.dateIndex = None
        self.timeIndex = None
        self.ecIndex = None
        self.remarksIndex = None

        self.site = None
        self.salt = None

    def batchMissingHeaders(self):
        if self.dateIndex == None:
            return True
        if self.timeIndex == None:
            return True
        if self.ecIndex == None:
            return True
        if self.remarksIndex == None:
            return True
        return False

    def resetValues(self):
        self.date = None
        self.time = None
        self.ec = None
        self.remarks = None

        self.site = None
        self.salt = None

    def getValues(self, row):
        if self.timeIndex != None:
            self.time = str(row[self.timeIndex])
        if self.dateIndex != None:
            self.date = str(row[self.dateIndex])
        if self.ecIndex != None:
            self.ec = row[self.ecIndex]
        if self.remarksIndex != None:
            self.remarks = row[self.remarksIndex]

    def replaceNan(self, value):
        if pd.isna(value):
            return None
        else:
            return value

    def cleanValues(self):
        self.date = self.replaceNan(self.date)
        self.time = self.replaceNan(self.time)
        self.ec = self.replaceNan(self.ec)
        self.remarks = self.replaceNan(self.remarks)

    def rowMissingValues(self):
        if self.date == None:
            return True
        if self.time == None:
            return True
        if self.ec == None:
            return True
        if self.remarks == None:
            return True

        return False

    def remarksAreValid(self):
        try:
            self.site, self.salt = self.remarks.split("-")
            if not self.salt.isnumeric():
                return False
            return True
        except:
            try:
                self.site, self.salt = self.remarks.split(" ")
                if not self.salt.isnumeric():
                    return False
                if not self.site.isalpha():
                    return False
                return True
            except:
                try:
                    self.site = self.remarks[:3]
                    self.salt = self.remarks[3:]
                    #self.site, self.salt = self.remarks.split(" ")
                    if not self.salt.isnumeric():
                        return False
                    if not self.site.isalpha():
                        return False
                    return True
                except:
                    return False

    def readRow(self, row):

        # get the data
        self.resetValues()
        self.getValues(row)
        self.cleanValues()

        # make sure there are no values missing
        if self.rowMissingValues():
            return self.missingValues

        # make sure the remarks are valid
        if self.remarksAreValid():
            return self.noErrors
        else:
            return self.invalidRemarks

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
            elif "ec" in header:
                self.ecIndex = i
            elif "remarks" in header:
                self.remarksIndex = i

            i = i + 1

        if self.batchMissingHeaders():
            return self.missingHeaders
        else:
            return self.noErrors

