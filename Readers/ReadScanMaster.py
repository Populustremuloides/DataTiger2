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
            i = i + 1



    def resetValues(self):
        self.sortChem = None
        self.timestamp = None

    def checkForNull(self):
        if self.sortChem == None:
            return self.error
        if self.timestamp == None:
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

        # check that the data was there, return success indicator
        return self.checkForNull()

