import pandas as pd
from datetime import datetime

class ReadDOCIsotopes:
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
        self.internalIdentifier = None
        self.doc = None
        self.delta13 = None
        self.comment = None

        self.sortChemIndex = None
        self.internalIdentifierIndex = None
        self.docIndex = None
        self.delta13Index = None
        self.commentIndex = None

    def resetValues(self):
        self.sortChem = None
        self.internalIdentifier = None
        self.doc = None
        self.delta13 = None
        self.comment = None

    def replaceBlankWithNull(self, value):
        if pd.isna(value):
            return None
        else:
            return value

    def headersMissingValues(self):
        if self.sortChemIndex == None:
            return True
        if self.internalIdentifierIndex == None:
            return True
        if self.docIndex == None:
            return True
        if self.delta13Index == None:
            return True
        if self.commentIndex == None:
            return True
        return False

    def readBatch(self, columns):
        self.datetimeUploaded = str(datetime.now())

        # register the column indices
        i = 0
        for column in columns:
            column = column.lower()

            if "sample" in column:
                self.sortChemIndex = i
            elif "internal" in column:
                self.internalIdentifierIndex = i
            elif "doc" in column:
                self.docIndex = i
            elif "13c" in column:
                self.delta13Index = i
            elif "comment" in column:
                self.commentIndex = i

            i = i + 1

        if self.headersMissingValues():
            return self.missingHeaders
        else:
            return self.noErrors


    def rowMissingValues(self):
        if self.sortChem == None:
            return True
        if self.docIndex == None:
            return True
        if self.delta13 == None:
            return True
        return False

    def cleanRow(self):
        self.sortChem = self.replaceBlankWithNull(self.sortChem)
        self.internalIdentifier = self.replaceBlankWithNull(self.internalIdentifier)
        self.doc = self.replaceBlankWithNull(self.doc)
        self.delta13 = self.replaceBlankWithNull(self.delta13)
        self.comment = self.replaceBlankWithNull(self.comment)


    def assignValues(self, row):
        if self.sortChemIndex != None:
            self.sortChem = row[self.sortChemIndex]
        if self.internalIdentifierIndex != None:
            self.internalIdentifier = row[self.internalIdentifierIndex]
        if self.docIndex != None:
            self.doc = row[self.docIndex]
        if self.delta13Index != None:
            self.delta13 = row[self.delta13Index]
        if self.commentIndex != None:
            self.comment = row[self.commentIndex]


    def readRow(self, row):

        self.resetValues()
        self.assignValues(row)
        self.cleanRow()

        if self.rowMissingValues():
            return self.missingValues
        else:
            return self.noErrors




