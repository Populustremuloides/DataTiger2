import pandas as pd
from datetime import datetime

class ReadTss:
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
        self.siteId = None

        self.userName = None
        self.dateCollected = None

        self.sampleVolume = None
        self.initialFilterWeight = None
        self.finalFilterWeight = None
        self.tss = None
        self.notes = None
        self.dryingNotes = None

        self.sortChemIndex = None
        self.siteIdIndex = None

        self.userNameIndex = None
        self.dateCollectedIndex = None

        self.sampleVolumeIndex = None
        self.initialFilterWeightIndex = None
        self.finalFilterWeightIndex = None
        self.tssIndex = None
        self.notesIndex = None
        self.dryingNotesIndex = None

    def resetValues(self):
        self.sortChem = None
        self.siteId = None

        self.userName = None
        self.dateCollected = None

        self.sampleVolume = None
        self.initialFilterWeight = None
        self.finalFilterWeight = None
        self.tss = None
        self.notes = None
        self.dryingNotes = None

    def replaceBlankWithNull(self, value):
        if pd.isna(value):
            return None
        else:
            return value

    def headersMissingValues(self):
        if self.sortChemIndex == None:
            return True
        if self.sampleVolumeIndex == None:
            return True
        if self.initialFilterWeightIndex == None:
            return True
        if self.finalFilterWeightIndex == None:
            return True
        if self.tssIndex == None:
            return True
        return False

    def readBatch(self, columns):
        self.datetimeUploaded = str(datetime.now())

        # register the column indices
        i = 0
        for column in columns:
            column = column.lower()

            if "sortchem" in column:
                self.sortChemIndex = i
            elif "site" in column:
                self.siteIdIndex = i
            elif "user" == column:
                self.userNameIndex = i
            elif "date" == column:
                self.dateCollectedIndex = i

            elif ("sample" in column) and ("volume" in column):
                self.sampleVolumeIndex = i
            elif "initial" in column and "filter" in column:
                self.initialFilterWeightIndex = i
            elif "final" in column and "filter" in column:
                self.finalFilterWeightIndex = i
            elif "tss" in column:
                self.tssIndex = i
            elif column == "notes":
                self.notesIndex = i
            elif "notes" in column and "drying" in column:
                self.dryingNotesIndex = i

            i = i + 1

        if self.headersMissingValues():
            return self.missingHeaders
        else:
            return self.noErrors


    def rowMissingValues(self):
        if self.sortChemIndex == None:
            return True
        if self.sampleVolumeIndex == None:
            return True
        if self.initialFilterWeightIndex == None:
            return True
        if self.finalFilterWeightIndex == None:
            return True
        if self.tssIndex == None:
            return True
        return False

    def cleanRow(self):
        self.sortChem = self.replaceBlankWithNull(self.sortChem)
        self.siteId = self.replaceBlankWithNull(self.siteId)
        # self.fixDate()

        self.userName = self.replaceBlankWithNull(self.userName)
        self.dateCollected = self.replaceBlankWithNull(self.dateCollected)

        self.sampleVolume = self.replaceBlankWithNull(self.sampleVolume)
        self.initialFilterWeight = self.replaceBlankWithNull(self.initialFilterWeight)
        self.finalFilterWeight = self.replaceBlankWithNull(self.finalFilterWeight)
        self.tss = self.replaceBlankWithNull(self.tss)
        self.notes = self.replaceBlankWithNull(self.notes)
        self.dryingNotes = self.replaceBlankWithNull(self.dryingNotes)

    def assignValues(self, row):

        if self.sortChemIndex != None:
            self.sortChem = row[self.sortChemIndex]
        if self.siteIdIndex != None:
            self.siteId = row[self.siteIdIndex]

        if self.userNameIndex != None:
            self.userName = row[self.userNameIndex]
        if self.dateCollectedIndex != None:
            self.dateCollected = row[self.dateCollectedIndex]
        if self.sampleVolumeIndex != None:
            self.sampleVolume = row[self.sampleVolumeIndex]

        if self.initialFilterWeightIndex != None:
            self.initialFilterWeight = row[self.initialFilterWeightIndex]
        if self.finalFilterWeightIndex != None:
            self.finalFilterWeight = row[self.finalFilterWeightIndex]
        if self.tssIndex != None:
            self.tss = row[self.tssIndex]
        if self.notesIndex != None:
            self.notes = row[self.notesIndex]
        if self.dryingNotesIndex != None:
            self.dryingNotes = row[self.dryingNotesIndex]

    def readRow(self, row):

        self.resetValues()
        self.assignValues(row)
        self.cleanRow()

        if self.rowMissingValues():
            return self.missingValues
        else:
            return self.noErrors



