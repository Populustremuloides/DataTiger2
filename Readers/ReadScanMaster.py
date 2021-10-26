import datetime
import pandas as pd

class ReadScanMaster:
    def __init__(self, filePath):
        self.filePath = filePath
        cleanPath = self.filePath.replace("\\", "/")
        cleanPath = cleanPath.replace("//", "/")
        pathList = cleanPath.split("/")
        self.fileName = pathList[-1]

        self.datetime = None
        self.sortChem = None
        self.datetimeValue = None
        self.turbidity = None
        self.no3 = None
        self.toc = None
        self.doc = None

        self.error = -1
        self.noError = 0

    def readBatch(self, columns):
        self.datetime = datetime.datetime.now()

        # get the indices of the different columns
        i = 0
        for column in columns:
            if "Chem #" in column:
                self.sortChemIndex = i
            elif "Timestamp" in column:
                self.timestampIndex = i
            elif "Turbidity" in column:
                self.turbidityIndex = i
            elif "NO3" in column:
                self.no3Index = i
            elif "TOC" in column:
                self.tocIndex = i
            elif "DOC" in column:
                self.docIndex = i

            i = i + 1

    def resetValues(self):
        self.sortChem = None
        self.datetimeValue = None
        self.turbidity = None
        self.no3 = None
        self.toc = None
        self.doc = None
        self.timestamp = None

    def checkForNull(self):
        if self.sortChem == None:
            return self.error
        if self.timestamp == None:
            return self.error
        if self.turbidity == None:
            return self.error
        if self.no3 == None:
            return self.error
        if self.toc == None:
            return self.error
        if self.doc == None:
            return self.error

        return self.noError

    def replaceNaWithNull(self, value):
        if pd.isna(value):
            return None
        else:
            return value

    def readDataRow(self, row):
        # reset the values
        self.resetValues()

        # get the data
        self.sortChem = self.replaceNaWithNull(row[self.sortChemIndex])
        self.timestamp = self.replaceNaWithNull(row[self.timestampIndex])
        self.turbidity = self.replaceNaWithNull(row[self.turbidityIndex])
        self.no3 = self.replaceNaWithNull(row[self.no3Index])
        self.doc = self.replaceNaWithNull(row[self.docIndex])
        self.toc = self.replaceNaWithNull(row[self.tocIndex])

        # check that the data was there, return success indicator
        return self.checkForNull()

