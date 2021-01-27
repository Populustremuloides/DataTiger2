import pandas as pd
from datetime import datetime

class ReadSites:
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

        self.site = None
        self.x = None
        self.y = None
        self.area = None
        self.bodyOfWater = None
        self.stationType = None

        self.siteIndex = None
        self.xIndex = None
        self.yIndex = None
        self.areaIndex = None
        self.bodyOfWaterIndex = None
        self.stationTypeIndex = None

    def resetValues(self):
        self.site = None
        self.x = None
        self.y = None
        self.area = None
        self.bodyOfWater = None
        self.stationType = None

    def replaceBlankWithNull(self, value):
        if pd.isna(value):
            return None
        else:
            return value

    def headersMissingValues(self):
        if self.siteIndex == None:
            return True
        if self.xIndex == None:
            return True
        if self.yIndex == None:
            return True
        if self.areaIndex == None:
            return True
        if self.bodyOfWaterIndex == None:
            return True
        if self.stationTypeIndex == None:
            return True
        return False

    def readBatch(self, columns):
        self.datetimeUploaded = str(datetime.now())

        # register the column indices
        i = 0
        for column in columns:
            column = column.lower()
            if "site" in column:
                self.siteIndex = i
            if column == "x":
                self.xIndex = i
            if column == "y":
                self.yIndex = i
            if "area" in column:
                self.areaIndex = i
            if "water" in column:
                self.bodyOfWaterIndex = i
            if "station" in column:
                self.stationTypeIndex = i

            i = i + 1

        if self.headersMissingValues():
            return self.missingHeaders
        else:
            return self.noErrors


    def rowMissingValues(self):
        if self.site == None:
            return True
        if self.x == None:
            return True
        if self.y == None:
            return True
        if self.area == None:
            return True
        if self.bodyOfWater == None:
            return True
        return False

    def cleanRow(self):
        self.site = self.replaceBlankWithNull(self.site)
        self.x = self.replaceBlankWithNull(self.x)
        self.y = self.replaceBlankWithNull(self.y)
        self.area = self.replaceBlankWithNull(self.area)
        self.bodyOfWater = self.replaceBlankWithNull(self.bodyOfWater)
        self.stationType = self.replaceBlankWithNull(self.stationType)

    def assignValues(self, row):

        if self.siteIndex != None:
            self.site = row[self.siteIndex]
            #self.site = self.site.replace(" ","")
        if self.xIndex != None:
            self.x = row[self.xIndex]
        if self.yIndex != None:
            self.y = row[self.yIndex]
        if self.areaIndex != None:
            self.area = row[self.areaIndex]
        if self.bodyOfWaterIndex != None:
            self.bodyOfWater = row[self.bodyOfWaterIndex]
        if self.stationTypeIndex != None:
            self.stationType = row[self.stationTypeIndex]


    def readRow(self, row):

        self.resetValues()
        self.assignValues(row)
        self.cleanRow()

        if self.rowMissingValues():
            return self.missingValues
        else:
            return self.noErrors


