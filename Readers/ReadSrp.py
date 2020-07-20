import pandas as pd
from datetime import datetime

class ReadSrp:
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

        self.absorbance = None
        self.concentration = None

        self.std1Concentration = None
        self.std2Concentration = None
        self.std3Concentration = None
        self.std4Concentration = None
        self.std5Concentration = None
        self.std6Concentration = None

        self.std1Absorbance = None
        self.std2Absorbance = None
        self.std3Absorbance = None
        self.std4Absorbance = None
        self.std5Absorbance = None
        self.std6Absorbance = None

        self.sortChemIndex = None
        self.analysisDateIndex = None

        self.absorbanceIndex = None
        self.concentrationIndex = None

        self.std1ConcentrationIndex = None
        self.std2ConcentrationIndex = None
        self.std3ConcentrationIndex = None
        self.std4ConcentrationIndex = None
        self.std5ConcentrationIndex = None
        self.std6ConcentrationIndex = None

        self.std1AbsorbanceIndex = None
        self.std2AbsorbanceIndex = None
        self.std3AbsorbanceIndex = None
        self.std4AbsorbanceIndex = None
        self.std5AbsorbanceIndex = None
        self.std6AbsorbanceIndex = None

    # def fixDate(self):
    #     if self.analysisDate != None:
    #         self.analysisDate = str(self.analysisDate)
    #         dateList = self.analysisDate.split("/")
    #
    #         month = str(dateList[0])
    #         day = str(dateList[1])
    #         year = str(dateList[2])
    #
    #         self.analysisDate = day + "-" + month + "-" + year

    def resetValues(self):
        self.sortChem = None
        self.analysisDate = None

        self.absorbance = None
        self.concentration = None

        self.std1Concentration = None
        self.std2Concentration = None
        self.std3Concentration = None
        self.std4Concentration = None
        self.std5Concentration = None
        self.std6Concentration = None

        self.std1Absorbance = None
        self.std2Absorbance = None
        self.std3Absorbance = None
        self.std4Absorbance = None
        self.std5Absorbance = None
        self.std6Absorbance = None

    def replaceBlankWithNull(self, value):
        if pd.isna(value):
            return None
        else:
            return value

    def headersMissingValues(self):
        if self.sortChemIndex == None:
            return True
        if self.absorbanceIndex == None:
            return True
        if self.concentrationIndex == None:
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
            elif "analysis date" in column:
                self.analysisDateIndex = i
            elif "absorbance" == column:
                self.absorbanceIndex = i
            elif "concentration" == column:
                self.concentrationIndex = i

            elif ("absorbance" in column) and ("1" in column):
                self.std1AbsorbanceIndex = i
            elif "absorbance" in column and "2" in column:
                self.std2AbsorbanceIndex = i
            elif "absorbance" in column and "3" in column:
                self.std3AbsorbanceIndex = i
            elif "absorbance" in column and "4" in column:
                self.std4AbsorbanceIndex = i
            elif "absorbance" in column and "5" in column:
                self.std5AbsorbanceIndex = i
            elif "absorbance" in column and "6" in column:
                self.std6AbsorbanceIndex = i

            elif "concentration" in column and "1" in column:
                self.std1ConcentrationIndex = i
            elif "concentration" in column and "2" in column:
                self.std2ConcentrationIndex = i
            elif "concentration" in column and "3" in column:
                self.std3ConcentrationIndex = i
            elif "concentration" in column and "4" in column:
                self.std4ConcentrationIndex = i
            elif "concentration" in column and "5" in column:
                self.std5ConcentrationIndex = i
            elif "concentration" in column and "6" in column:
                self.std6ConcentrationIndex = i

            i = i + 1

        if self.headersMissingValues():
            return self.missingHeaders
        else:
            return self.noErrors


    def rowMissingValues(self):
        if self.sortChem == None:
            return True
        if self.concentration == None:
            return True
        if self.absorbance == None:
            return True
        return False

    def cleanRow(self):
        self.sortChem = self.replaceBlankWithNull(self.sortChem)
        self.analysisDate = self.replaceBlankWithNull(self.analysisDate)
        # self.fixDate()

        self.absorbance = self.replaceBlankWithNull(self.absorbance)
        self.concentration = self.replaceBlankWithNull(self.concentration)

        self.std1Concentration = self.replaceBlankWithNull(self.std1Concentration)
        self.std2Concentration = self.replaceBlankWithNull(self.std2Concentration)
        self.std3Concentration = self.replaceBlankWithNull(self.std3Concentration)
        self.std4Concentration = self.replaceBlankWithNull(self.std4Concentration)
        self.std5Concentration = self.replaceBlankWithNull(self.std5Concentration)
        self.std6Concentration = self.replaceBlankWithNull(self.std6Concentration)

        self.std1Absorbance = self.replaceBlankWithNull(self.std1Absorbance)
        self.std2Absorbance = self.replaceBlankWithNull(self.std2Absorbance)
        self.std3Absorbance = self.replaceBlankWithNull(self.std3Absorbance)
        self.std4Absorbance = self.replaceBlankWithNull(self.std4Absorbance)
        self.std5Absorbance = self.replaceBlankWithNull(self.std5Absorbance)
        self.std6Absorbance = self.replaceBlankWithNull(self.std6Absorbance)


    def assignValues(self, row):

        if self.sortChemIndex != None:
            self.sortChem = row[self.sortChemIndex]
        if self.analysisDateIndex != None:
            self.analysisDate = row[self.analysisDateIndex]

        if self.absorbanceIndex != None:
            self.absorbance = row[self.absorbanceIndex]
        if self.concentrationIndex != None:
            self.concentration = row[self.concentrationIndex]

        if self.std1ConcentrationIndex != None:
            self.std1Concentration = row[self.std1ConcentrationIndex]
        if self.std2ConcentrationIndex != None:
            self.std2Concentration = row[self.std2ConcentrationIndex]
        if self.std3ConcentrationIndex != None:
            self.std3Concentration = row[self.std3ConcentrationIndex]
        if self.std4ConcentrationIndex != None:
            self.std4Concentration = row[self.std4ConcentrationIndex]
        if self.std5ConcentrationIndex != None:
            self.std5Concentration = row[self.std4ConcentrationIndex]
        if self.std6ConcentrationIndex != None:
            self.std6Concentration = row[self.std6ConcentrationIndex]

        if self.std1AbsorbanceIndex != None:
            self.std1Absorbance = row[self.std1AbsorbanceIndex]
        if self.std2AbsorbanceIndex != None:
            self.std2Absorbance = row[self.std2AbsorbanceIndex]
        if self.std3AbsorbanceIndex != None:
            self.std3Absorbance = row[self.std3AbsorbanceIndex]
        if self.std4AbsorbanceIndex != None:
            self.std4Absorbance = row[self.std4AbsorbanceIndex]
        if self.std5AbsorbanceIndex != None:
            self.std5Absorbance = row[self.std5AbsorbanceIndex]
        if self.std6AbsorbanceIndex != None:
            self.std6Absorbance = row[self.std6AbsorbanceIndex]


    def readRow(self, row):

        self.resetValues()
        self.assignValues(row)
        self.cleanRow()

        if self.rowMissingValues():
            return self.missingValues
        else:
            return self.noErrors



