import pandas as pd
from datetime import datetime

class ReadLachat:
    def __init__(self, filePath):
        self.filePath = filePath

        cleanPath = self.filePath.replace("\\", "/")
        cleanPath = cleanPath.replace("//", "/")
        pathList = cleanPath.split("/")
        self.fileName = pathList[-1]

        self.noErrors = 0
        self.missingValues = -1
        self.missingHeaders = -2

        self.sortChem = None
        self.no3 = None
        self.no4 = None

        self.sortChemIndex = None
        self.no3Index = None
        self.no4Index = None

    def resetValues(self):
        self.sortChem = None
        self.no3 = None
        self.no4 = None

    def replaceBlankWithNull(self, value):
        if pd.isna(value):
            return None
        else:
            return value

    def cleanRow(self):
        # replace nan values with Null values
        self.sortChem = self.replaceBlankWithNull(self.sortChem)
        self.no3 = self.replaceBlankWithNull(self.no3)
        self.no4 = self.replaceBlankWithNull(self.no4)

    def readBatch(self, headers):
        # get the datetime
        self.datetimeUploaded = str(datetime.now())

        # register the indices of the headers
        i = 0
        for header in headers:
            if not pd.isna(header):
                header = header.lower()
                if "sample" in header:
                    self.sortChemIndex = i
                if "3" in header or u"\u2083" in header:
                    if "ppm" in header:
                        self.no3Index = i
                if "4" in header or u"\u2084" in header:
                    if "ppm" in header:
                        self.no4Index = i
            i = i + 1

        if self.nullHeaders():
            return self.missingHeaders
        else:
            return self.noErrors

    def nullHeaders(self):
        if self.sortChemIndex == None:
            return True
        if self.no3Index == None:
            return True
        if self.no4Index == None:
            return True
        return False

    def rowMissingValues(self):
        if self.sortChem == None:
            return True
        if self.no3 == None:
            return True
        if self.no4 == None:
            return True
        return False


    def readRow(self, row):

        self.resetValues()

        if self.sortChemIndex != None:
            self.sortChem = row[self.sortChemIndex]
        if self.no3Index != None:
            self.no3 = row[self.no3Index]
        if self.no4Index != None:
            self.no4 = row[self.no4Index]

        self.cleanRow()

        if self.rowMissingValues():
            return self.missingValues
        else:
            return self.noErrors


