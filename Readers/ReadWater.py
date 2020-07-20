import pandas as pd
from datetime import datetime

class ReadWater:
    def __init__(self, filePath):
        self.filePath = filePath

        cleanPath = self.filePath.replace("\\", "/")
        cleanPath = cleanPath.replace("//", "/")
        pathList = cleanPath.split("/")
        self.fileName = pathList[-1]

        self.noErrors = 0
        self.missingValues = -1
        self.missingHeaders = -2

        self.datetimeUploaded = None

        self.sortChem = None
        self.analysisDate = None
        self.d18O = None
        self.d18OError = None
        self.dD = None
        self.dDError = None

        self.sortChemIndex = None
        self.analysisDateIndex = None
        self.d18OIndex = None
        self.d18OErrorIndex = None
        self.dDIndex = None
        self.dDErrorIndex = None


    def resetValues(self):
        self.sortChem = None
        self.analysisDate = None
        self.d18O = None
        self.d18OError = None
        self.dD = None
        self.dDError = None

    def replaceBlankWithNull(self, value):
        if pd.isna(value):
            return None
        else:
            return value

    def headersMissingValues(self):
        if self.sortChemIndex == None:
            return True
        if self.d18OIndex == None:
            return True
        if self.dDIndex == None:
            return True
        return False

    def readBatch(self, columns):
        self.datetimeUploaded = str(datetime.now())

        # register the column indices
        i = 0
        for column in columns:
            column = column.lower()
            if "site" in column:
                self.sortChemIndex = i
            elif "date" in column:
                self.analysisDateIndex = i
            elif "18" in column:
                self.d18OIndex = i
                self.d18OErrorIndex = i + 1
            elif "dd" in column:
                self.dDIndex = i
                self.dDErrorIndex = i + 1

            i = i + 1

        if self.headersMissingValues():
            return self.missingHeaders
        else:
            return self.noErrors


    def rowMissingValues(self):
        if self.sortChem == None:
            return True
        if self.d18O == None:
            return True
        if self.dD == None:
            return True
        return False

    def cleanRow(self):
        self.sortChem = self.replaceBlankWithNull(self.sortChem)
        self.analysisDate = self.replaceBlankWithNull(self.analysisDate)
        if self.analysisDate != None:
            self.analysisDate = str(self.analysisDate)
        self.d18O = self.replaceBlankWithNull(self.d18O)
        self.d18OError = self.replaceBlankWithNull(self.d18OError)
        self.dD = self.replaceBlankWithNull(self.dD)
        self.dDError = self.replaceBlankWithNull(self.dDError)

    def assignValues(self, row):

        if self.sortChemIndex != None:
            self.sortChem = row[self.sortChemIndex]
        if self.analysisDateIndex != None:
            self.analysisDate = row[self.analysisDateIndex]
        if self.d18OIndex != None:
            self.d18O = row[self.d18OIndex]
        if self.d18OErrorIndex != None:
            self.d18OError = row[self.d18OErrorIndex]
        if self.dDIndex != None:
            self.dD = row[self.dDIndex]
        if self.dDErrorIndex != None:
            self.dDError = row[self.dDErrorIndex]


    def readRow(self, row):

        self.resetValues()
        self.assignValues(row)
        self.cleanRow()

        if self.rowMissingValues():
            return self.missingValues
        else:
            return self.noErrors


