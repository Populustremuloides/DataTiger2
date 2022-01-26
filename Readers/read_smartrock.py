from datetime import datetime
import pandas as pd
import re

class read_smartrock:
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

        self.datetime = None

        self.ec = None
        self.turbidity = None
        self.pressure = None
        self.temp = None

        self.dateIndex = -2
        self.timeIndex = -1

        self.ecIndex = None
        self.turbidityIndex = None
        self.pressureIndex = None
        self.tempIndex = None

    def batchMissingHeaders(self):
        if self.ecIndex == None:
            return True
        if self.turbidityIndex == None:
            return True
        if self.pressureIndex == None:
            return True
        if self.tempIndex == None:
            return True

        return False

    def clearValues(self):
        self.date = None
        self.time = None
        self.ec = None
        self.turbidity = None
        self.pressure = None
        self.temp = None

    def getValues(self, row):
        self.clearValues()

        if not self.dateIndex is None:
            self.date = row[self.dateIndex]
        if not self.timeIndex is None:
            self.time = row[self.timeIndex]
            
        if not self.ecIndex is None:
            self.ec = row[self.ecIndex]
        if self.turbidityIndex:
            self.turbidity = row[self.turbidityIndex]
        if self.pressureIndex:
            self.pressure = row[self.pressureIndex]
        if self.tempIndex:
            self.temp = row[self.tempIndex]

    def replaceNan(self, value):
        if pd.isna(value):
            return None
        else:
            return value

    def rowMissingValues(self):
        if self.date == None:
            return True
        if self.time == None:
            return True
        if self.ec == None:
            return True
        if self.turbidity == None:
            return True
        if self.pressure == None:
            return True
        if self.temp == None:
            return True

        return False

    def readRow(self, row):
        # get the data
        self.getValues(row)

        # make sure there are no values missing
        if self.rowMissingValues():
            return self.missingValues

        return self.noErrors

    def readBatch(self, headers, preheaders):

        nameList = self.fileName.split(".")[0]

        try:
            self.projectId = re.match(r"^([a-zA-Z]*)(\d*)", nameList).groups()[0]
            self.runDate = re.match(r"^[a-zA-Z]*(\d*)", nameList).groups()[0]
        except:
            raise smartrockFileNotNamedCorrectly(self.fileName)

        self.projectId = self.projectId.replace(" ", "")

        # get the datetime uploaded
        self.datetimeUploaded = str(datetime.now())

        for index in range(len(headers)):
            column = headers[index]
            preheader = preheaders[index] if type(preheaders[index]) == str else None
            column = column.lower() if type(column) == str else None

            if not column:
                break
            elif "analog0" in column:
                self.ecIndex = index
            elif "analog2" in column:
                self.turbidityIndex = index
            elif "pressure" in column:
                self.pressureIndex = index
            elif "temp" in column:
                self.tempIndex = index
            elif "date" in column and "localtime" in preheader:
                self.timeIndex = index

        if self.batchMissingHeaders():
            return self.missingHeaders
        else:
            return self.noErrors
