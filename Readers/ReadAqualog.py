import pandas as pd

class ReadAqualog:
    def __init__(self, filePath):
        self.filePath = filePath

        cleanPath = self.filePath.replace("\\", "/")
        cleanPath = cleanPath.replace("//", "/")
        pathList = cleanPath.split("/")
        self.fileName = pathList[-1]

        self.noErrors = 0
        self.missingValues = -1
        self.missingHeaders = -2

        self.operator = None
        self.correctedBy = None
        self.dateCollected = None
        self.dateCorrected = None
        self.projectFile = None
        self.sampleName = None
        self.sortChem = None
        self.correctedName = None
        self.blankName = None
        self.dilutionFactor = None
        self.notes = None
        self.fi370 = None
        self.fi310 = None
        self.fi254 = None
        self.abs254 = None
        self.slp274_295 = None
        self.slp350_400 = None
        self.sr = None
        self.ese3 = None

        self.operatorIndex = None
        self.correctedByIndex = None
        self.dateCollectedIndex = None
        self.dateCorrectedIndex = None
        self.projectFileIndex = None
        self.sampleNameIndex = None
        self.sortChemIndex = None
        self.correctedNameIndex = None
        self.blankNameIndex = None
        self.dilutionFactorIndex = None
        self.notesIndex = None
        self.fi370Index = None
        self.fi310Index = None
        self.fi254Index = None
        self.abs254Index = None
        self.slp274_295Index = None
        self.slp350_400Index = None
        self.srIndex = None
        self.ese3Index = None


    def resetValues(self):
        self.operator = None
        self.correctedBy = None
        self.dateCollected = None
        self.dateCorrected = None
        self.projectFile = None
        self.sampleName = None
        self.sortChem = None
        self.correctedName = None
        self.blankName = None
        self.dilutionFactor = None
        self.notes = None
        self.fi370 = None
        self.fi310 = None
        self.fi254 = None
        self.abs254 = None
        self.slp274_295 = None
        self.slp350_400 = None
        self.sr = None
        self.ese3 = None


    def readBatch(self, headers):
        # get the indices of the different values
        i = 0
        for header in headers:
            header = header.lower()
            if "operator" in header:
                self.operatorIndex = i
            elif ("corrected" in header) and ("by" in header):
                self.correctedByIndex = i
            elif "collected" in header:
                self.dateCollectedIndex = i
            elif ("date" in header) and ("corrected" in header):
                self.dateCorrectedIndex = i
            elif ("project" in header) and ("file" in header):
                self.projectFileIndex = i
            elif ("sample" in header) and ("name" in header):
                self.sampleNameIndex = i
            elif ("sample" in header) and ("id" in header):
                self.sortChemIndex = i
            elif ("corrected" in header) and ("name" in header):
                self.correctedNameIndex = i
            elif ("blank" in header) and ("name" in header):
                self.blankNameIndex = i
            elif "dilution" in header:
                self.dilutionFactorIndex = i
            elif "notes" in header:
                self.notesIndex = i
            elif "fi370" in header:
                self.fi370Index = i
            elif "fi310" in header:
                self.fi310Index = i
            elif "fi254" in header:
                self.fi254Index = i
            elif "abs254" in header:
                self.abs254Index = i
            elif "slp274" in header:
                self.slp274_295Index = i
            elif "slp350" in header:
                self.slp350_400Index = i
            elif "sr" in header:
                self.srIndex = i
            elif "es_e3" in header:
                self.ese3Index = i

            i = i + 1

        if self.nullIndicesPresent():
            return self.missingValues
        else:
            return self.noErrors


    def replaceBlankWithNull(self, value):
        if pd.isna(value):
            return None
        else:
            return value


    def nullIndicesPresent(self):
        if self.operatorIndex == None:
            return True
        if self.correctedByIndex == None:
            return True
        if self.dateCollectedIndex == None:
            return True
        if self.dateCorrectedIndex == None:
            return True
        if self.projectFileIndex == None:
            return True
        if self.sampleNameIndex == None:
            return True
        if self.sortChemIndex == None:
            return True
        if self.correctedNameIndex == None:
            return True
        if self.blankNameIndex == None:
            return True
        if self.dilutionFactorIndex == None:
            return True
        if self.notesIndex == None:
            return True
        if self.fi370Index == None:
            return True
        if self.fi310Index == None:
            return True
        if self.fi254Index == None:
            return True
        if self.abs254Index == None:
            return True
        if self.slp274_295Index == None:
            return True
        if self.slp350_400Index == None:
            return True
        if self.srIndex == None:
            return True
        if self.ese3Index == None:
            return True

        return False


    def cleanValues(self):

        self.operator = self.replaceBlankWithNull(self.operator)
        self.correctedBy = self.replaceBlankWithNull(self.correctedBy)
        self.dateCollected = self.replaceBlankWithNull(self.dateCollected)
        self.dateCorrected = self.replaceBlankWithNull(self.dateCorrected)
        self.projectFile = self.replaceBlankWithNull(self.projectFile)
        self.sampleName = self.replaceBlankWithNull(self.sampleName)
        self.sortChem = self.replaceBlankWithNull(self.sortChem)
        self.correctedName = self.replaceBlankWithNull(self.correctedName)
        self.blankName = self.replaceBlankWithNull(self.blankName)
        self.dilutionFactor = self.replaceBlankWithNull(self.dilutionFactor)
        self.notes = self.replaceBlankWithNull(self.notes)
        self.fi370 = self.replaceBlankWithNull(self.fi370)
        self.fi310 = self.replaceBlankWithNull(self.fi310)
        self.fi254 = self.replaceBlankWithNull(self.fi254)
        self.abs254 = self.replaceBlankWithNull(self.abs254)
        self.slp274_295 = self.replaceBlankWithNull(self.slp274_295)
        self.slp350_400 = self.replaceBlankWithNull(self.slp350_400)
        self.sr = self.replaceBlankWithNull(self.sr)
        self.ese3 = self.replaceBlankWithNull(self.ese3)

        if self.dateCollected != None:
            self.dateCollected = str(self.dateCollected)
        if self.dateCorrected != None:
            self.dateCorrected = str(self.dateCorrected)

    def rowMissingValues(self):
        if self.sortChem == None:
            return True

        if self.fi370 == None:
            return True
        if self.fi310 == None:
            return True
        if self.fi254 == None:
            return True
        if self.abs254 == None:
            return True
        if self.slp274_295 == None:
            return True
        if self.slp350_400 == None:
            return True
        if self.sr == None:
            return True
        if self.ese3 == None:
            return True

        return False


    def readRow(self, row):
        # reset from previous rows
        self.resetValues()

        # get the values for the row
        if self.operatorIndex != None:
            self.operator = row[self.operatorIndex]
        if self.correctedByIndex != None:
            self.correctedBy = row[self.correctedByIndex]
        if self.dateCollectedIndex != None:
            self.dateCollected = row[self.dateCollectedIndex]
        if self.dateCorrectedIndex != None:
            self.dateCorrected = row[self.dateCorrectedIndex]
        if self.projectFileIndex != None:
            self.projectFile = row[self.projectFileIndex]
        if self.sampleNameIndex != None:
            self.sampleName = row[self.sampleNameIndex]
        if self.sortChemIndex != None:
            self.sortChem = row[self.sortChemIndex]
        if self.correctedNameIndex != None:
            self.correctedName = row[self.correctedNameIndex]
        if self.blankNameIndex != None:
            self.blankName = row[self.blankNameIndex]
        if self.dilutionFactorIndex != None:
            self.dilutionFactor = row[self.dilutionFactorIndex]
        if self.notesIndex != None:
            self.notes = row[self.notesIndex]
        if self.fi370Index != None:
            self.fi370 = row[self.fi370Index]
        if self.fi310Index != None:
            self.fi310 = row[self.fi310Index]
        if self.fi254Index != None:
            self.fi254 = row[self.fi254Index]
        if self.abs254Index != None:
            self.abs254 = row[self.abs254Index]
        if self.slp274_295Index != None:
            self.slp274_295 = row[self.slp274_295Index]
        if self.slp350_400Index != None:
            self.slp350_400 = row[self.slp350_400Index]
        if self.srIndex != None:
            self.sr = row[self.srIndex]
        if self.ese3Index != None:
            self.ese3 = row[self.ese3Index]

        # replace blanks with null values
        self.cleanValues()

        # check for missing values on critical rows
        if self.rowMissingValues():
            return self.missingValues
        else:
            return self.noErrors



