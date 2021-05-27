from datetime import datetime
import pandas as pd

class ReadNewSrp:
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

        self.sortchem = None
        self.no3 = None
        self.nh4 = None
        self.srp = None

        self.sortchemIndex = None
        self.no3Index = None
        self.nh4Index = None
        self.srpIndex = None

    def batchMissingHeaders(self):
        if self.sortchemIndex == None:
            return True
        if self.no3Index == None:
            return True
        if self.nh4Index == None:
            return True
        if self.srpIndex == None:
            return True

        return False

    def clearValues(self):
        self.sortchem = None
        self.no3 = None
        self.nh4 = None
        self.srp = None

    def getValues(self, row):
        self.clearValues()

        if not self.sortchemIndex is None:
            self.sortchem = row[self.sortchemIndex]
        if self.no3Index:
            self.no3 = row[self.no3Index]
        if self.srpIndex:
            self.srp = row[self.srpIndex]
        if self.nh4Index:
            self.nh4 = row[self.nh4Index]

    def replaceNan(self, value):
        if pd.isna(value):
            return None
        else:
            return value

    def rowMissingValues(self):
        if self.srp == None:
            return True
        if self.sortchem == None:
            return True
        if self.no3 == None:
            return True
        if self.nh4 == None:
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

        nameList = self.fileName.split(".")
        try:
            self.runDate = nameList[0]
            self.projectId = nameList[1]
        except:
            raise NewSRPFileNotNamedCorrectly(self.fileName)

        self.projectId = self.projectId.replace(" ", "")

        # get the datetime uploaded
        self.datetimeUploaded = str(datetime.now())

        for index in range(len(headers)):
            column = headers[index]
            preheader = preheaders[index]
            column = column.lower() if type(column) == str else None
            preheader = preheader.lower() if type(preheader) == str else None

            if not column:
                print("skipped this column")
                print(column)
                break
            elif ("sort" in column and "chem" in column) or ("sample" in column and "id" in column):
                self.sortchemIndex = index
            elif "ppm" in column and "no₃⁻" in column and index > 0:
                self.no3Index = index
            elif "ppm" in column and "nh" in column and index > 0:
                self.nh4Index = index
            elif "ppm" in column and preheader and "soluble reactive p" in preheader and index > 0:
                self.srpIndex = index
            else:
                print("skipped this column")
                print(column)

        if self.batchMissingHeaders():
            return self.missingHeaders
        else:
            return self.noErrors
