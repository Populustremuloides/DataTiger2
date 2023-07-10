from CustomErrors import *
import pandas as pd
import re

class ReadICPNew:
    def __init__(self, filePath):
        self.filePath = filePath
        cleanPath = self.filePath.replace("\\", "/")
        cleanPath = cleanPath.replace("//", "/")
        pathList = cleanPath.split("/")
        self.fileName = pathList[-1]

        self.sortChemIndex = None
        self.sortChem = None

        self.aluminumIndex = None
        self.arsenicIndex = None
        self.boronIndex = None
        self.bariumIndex = None
        self.calciumIndex = None
        self.cadmiumIndex = None
        self.cobaltIndex = None
        self.chromiumIndex = None
        self.copperIndex = None
        self.ironIndex = None
        self.potassiumIndex = None
        self.magnesiumIndex = None
        self.manganeseIndex = None
        self.molybdenumIndex = None
        self.sodiumIndex = None
        self.nickelIndex = None
        self.phosphorusIndex = None
        self.leadIndex = None
        self.sulfurIndex = None
        self.seleniumIndex = None
        self.siliconIndex = None
        self.strontiumIndex = None
        self.titaniumIndex = None
        self.vanadiumIndex = None
        self.zincIndex = None

        self.aluminum = None
        self.arsenic = None
        self.boron = None
        self.barium = None
        self.calcium = None
        self.cadmium = None
        self.cobalt = None
        self.chromium = None
        self.copper = None
        self.iron = None
        self.potassium = None
        self.magnesium = None
        self.manganese = None
        self.molybdenum = None
        self.sodium = None
        self.nickel = None
        self.phosphorus = None
        self.lead = None
        self.sulfur = None
        self.selenium = None
        self.silicon = None
        self.strontium = None
        self.titanium = None
        self.vanadium = None
        self.zinc = None




    def getFileName(self):
        return self.fileName

    def getFilePath(self):
        return self.filePath

    def readBatch(self, columns):
        # get the filename
        nameList = self.fileName.split(" ")

        try:
            self.runDate = nameList[0]
            self.projectId = nameList[1]
            self.operator = "Unknown"
        except:
            raise ICPFileNotNamedCorrectly(self.fileName)

        # fix the date
        year = self.runDate[0:2]
        month = self.runDate[2:4]
        day = self.runDate[4:6]
        year = "20" + year
        self.runDate = year + "-" + month + "-" + day

        self.sortChemIndex = 1

        for index in range(len(columns)):
            column = columns[index]
            column = column.lower() if type(column) == str else None

            #if not column:
                #print("skipped this column")
                #print(column)

            if column == "al1670":
                self.aluminumIndex = index
            elif column == "as1890":
                self.arsenicIndex = index
            elif column == "b_2497":
                self.boronIndex = index
            elif column == "ba4554":
                self.bariumIndex = index
            elif column == "ca3179":
                self.calciumIndex = index
            elif column == "cd2265":
                self.cadmiumIndex = index
            elif column == "co2286":
                self.cobaltIndex = index
            elif column == "cr2835":
                self.chromiumIndex = index
            elif column == "cu3247":
                self.copperIndex = index
            elif column == "fe2382":
                self.ironIndex = index
            elif column == "k_7664":
                self.potassiumIndex = index
            elif column == "mg2852":
                self.magnesiumIndex = index
            elif column == "mn2576":
                self.manganeseIndex = index
            elif column == "mo2020":
                self.molybdenumIndex = index
            elif column == "na5895":
                self.sodiumIndex = index
            elif column == "ni2316":
                self.nickelIndex = index
            elif column == "p_1782":
                self.phosphorusIndex = index
            elif column == "pb2203":
                self.leadIndex = index
            elif column == "s_1820":
                self.sulfurIndex = index
            elif column == "se1960":
                self.seleniumIndex = index
            elif column == "si2516":
                self.siliconIndex = index
            elif column == "sr4077":
                self.strontiumIndex = index
            elif column == "ti3088":
                self.titaniumIndex = index
            elif column == "v_3093":
                self.vanadiumIndex = index
            elif column == "zn2138":
                self.zincIndex = index
            else:
                print("skipped this column")
                print(column)


    def clearValues(self):
        self.sortChem = None
        self.aluminum = None
        self.arsenic = None
        self.boron = None
        self.barium = None
        self.calcium = None
        self.cadmium = None
        self.cobalt = None
        self.chromium = None
        self.copper = None
        self.iron = None
        self.potassium = None
        self.magnesium = None
        self.manganese = None
        self.molybdenum = None
        self.sodium = None
        self.nickel = None
        self.phosphorus = None
        self.lead = None
        self.sulfur = None
        self.selenium = None
        self.silicon = None
        self.strontium = None
        self.titanium = None
        self.vanadium = None
        self.zinc = None


    def readRow(self, row):
        self.clearValues()

        if self.sortChemIndex != None:
            sortchem = re.sub(r".*(\d{4}-\d{4}).*", r"\1", row[self.sortChemIndex])
            self.sortChem = sortchem

        if self.aluminumIndex != None:
            self.aluminum = row[self.aluminumIndex]
        else:
            print("FAIL")

        if self.arsenicIndex != None:
            self.arsenic = row[self.arsenicIndex]

        if self.boronIndex != None:
            self.boron = row[self.boronIndex]

        if self.bariumIndex != None:
            self.barium = row[self.bariumIndex]

        if self.calciumIndex != None:
            self.calcium= row[self.calciumIndex]

        if self.cadmiumIndex != None:
            self.cadmium = row[self.cadmiumIndex]

        if self.cobaltIndex != None:
            self.cobalt = row[self.cobaltIndex]

        if self.chromiumIndex != None:
            self.chromium = row[self.chromiumIndex]

        if self.copperIndex != None:
            self.copper = row[self.copperIndex]

        if self.ironIndex != None:
            self.iron = row[self.ironIndex]

        if self.potassiumIndex != None:
            self.potassium = row[self.potassiumIndex]

        if self.magnesiumIndex != None:
            self.magnesium = row[self.magnesiumIndex]

        if self.manganeseIndex != None:
            self.manganese = row[self.manganeseIndex]

        if self.molybdenumIndex != None:
            self.molybdenum = row[self.molybdenumIndex]

        if self.sodiumIndex != None:
            self.sodium = row[self.sodiumIndex]

        if self.nickelIndex != None:
            self.nickel = row[self.nickelIndex]

        if self.phosphorusIndex != None:
            self.phosphorus = row[self.phosphorusIndex]

        if self.leadIndex != None:
            self.lead = row[self.leadIndex]

        if self.sulfurIndex != None:
            self.sulfur = row[self.sulfurIndex]

        if self.seleniumIndex != None:
            self.selenium = row[self.seleniumIndex]

        if self.siliconIndex != None:
            self.silicon = row[self.siliconIndex]

        if self.strontiumIndex != None:
            self.strontium = row[self.strontiumIndex]

        if self.titaniumIndex != None:
            self.titanium = row[self.titaniumIndex]

        if self.vanadiumIndex != None:
            self.vanadium = row[self.vanadiumIndex]

        if self.zincIndex != None:
            self.zinc = row[self.zincIndex]
