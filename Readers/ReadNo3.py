import pandas as pd
from datetime import datetime
class ReadNo3:
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
        self.delta15 = None
        self.delta18 = None
        self.notes = None

        self.sortChemIndex = None
        self.delta15Index = None
        self.delta18Index = None
        self.notesIndex = None

    def resetValues(self):
        self.sortChem = None
        self.delta15 = None
        self.delta18 = None
        self.notes = None



    def readBatch(self, headers):
        self.datetimeUploaded = str(datetime.now())

        i = 0
        for header in headers:
            header = header.lower()
            if "id" in header:
                self.sortChemIndex = i
            elif "air" in header:
                self.delta15Index = i
            elif "ovsm" in header:
                self.delta18Index = i
            elif "notes" in header:
                self.notesIndex = i

            i = i + 1

        print(self.sortChemIndex)
        print(self.delta15Index)
        print(self.delta18Index)
        print(self.notesIndex)

    def replaceBlankWithNull(self, value):
        if pd.isna(value):
            return None
        else:
            return value

    def headersMissingValues(self):
        if self.sortChemIndex == None:
            return True
        if self.delta15Index == None:
            return True
        if self.delta18Index == None:
            return True
        if self.notesIndex == None:
            return True
        return False

    def rowMissingValues(self):
        if self.sortChem == None:
            return True
        if self.delta15 == None:
            return True
        if self.delta18 == None:
            return True
        return False


    def cleanRow(self):
        self.sortChem = self.replaceBlankWithNull(self.sortChem)
        self.delta15 = self.replaceBlankWithNull(self.delta15)
        self.delta18 = self.replaceBlankWithNull(self.delta18)
        self.notes = self.replaceBlankWithNull(self.notes)

    def readRow(self, row):
        self.resetValues()

        if self.sortChemIndex != None:
            self.sortChem = row[self.sortChemIndex]
        if self.delta15Index != None:
            self.delta15 = row[self.delta15Index]
        if self.delta18Index != None:
            self.delta18 = row[self.delta18Index]
        if self.notesIndex != None:
            self.notes = row[self.notesIndex]

        self.cleanRow()

        if self.rowMissingValues():
            return self.missingValues
        else:
            return self.noErrors


