
# for the old one
# make a dictionary of the headers, associate an index with the column number

# for each row
    # manually go through the dictionary

# For the new one:
# locate the amount column, locate how much space till the next header.
# for those indeces, go through and make a dictionary

from CustomErrors import *
import pandas as pd
class ReadIC():
    def __init__(self, filePath):

        self.filePath = filePath
        cleanPath = self.filePath.replace("\\", "/")
        cleanPath = cleanPath.replace("//", "/")
        pathList = cleanPath.split("/")
        self.fileName = pathList[-1]

        self.fluorideIndex = None
        self.acetateIndex = None
        self.formateIndex = None
        self.chlorideIndex = None
        self.nitriteIndex = None
        self.bromideIndex = None
        self.nitrateIndex = None
        self.sulfateIndex = None
        self.phosphateIndex = None

        self.lithiumIndex = None
        self.sodiumIndex = None
        self.ammoniumIndex = None
        self.potassiumIndex = None
        self.magnesiumIndex = None
        self.calciumIndex = None
        self.strontiumIndex = None

        self.fluoride = None
        self.acetate = None
        self.formate = None
        self.chloride = None
        self.nitrite = None
        self.bromide = None
        self.nitrate = None
        self.sulfate = None
        self.phosphate = None
        self.anionDate = None
        self.anionTime = None

        self.lithium = None
        self.sodium = None
        self.ammonium = None
        self.potassium = None
        self.magnesium = None
        self.calcium = None
        self.strontium = None
        self.cationDate = None
        self.cationTime = None

        self.rowContainsAnion = False
        self.rowContainsCation = False

        self.anionNameIndex = None
        self.cationNameIndex = None

    def readBatch(self, headers):
        # get the filename
        fileNameList = self.fileName.split(".")

        if len(fileNameList) != 4:
            raise ICPFileNotNamedCorrectly(self.fileName)

        self.runDate = fileNameList[0]
        self.projectId = fileNameList[1]
        self.operator = fileNameList[2]

        self.projectId = self.projectId.replace(" ","")


        # fix the date
        year = self.runDate[0:2]
        month = self.runDate[2:4]
        day = self.runDate[4:6]
        year = "20" + year
        self.runDate = year + "-" + month + "-" + day


        # get the number of names
        names = []
        i = 0
        for column in headers:
            column = column.lower()
            headers[i] = column
            if "name" in column and "unnamed" not in column:
                names.append(column)
            i = i + 1

        cations = [
            "lithium",
            "sodium",
            "ammonium",
            "potassium",
            "magnesium",
            "calcium",
            "strontium"
        ]
        anions = [
            "fluoride",
            "acetate",
            "formate",
            "chloride",
            "nitrtie",
            "bromide",
            "nitrate",
            "sulfate",
            "phosphate"
        ]

        i = 0
        for column in headers:
            column = column.lower()

            if "name" in column and "unnamed" not in column: # grab the name (sort chem)
                if i + 3 < len(headers):
                    if headers[i + 3].lower() in anions:
                        self.anionNameIndex = i
                    elif headers[i + 3].lower() in cations:
                        self.cationNameIndex = i
                    else:
                        print('DIDNT ASSIGN NAME INDEX CORRECTLY')
            if "lithium" in column:
                self.lithiumIndex = i
            elif "sodium" in column:
                self.sodiumIndex = i
            elif "ammonium" in column:
                self.ammoniumIndex = i
            elif "potassium" in column:
                self.potassiumIndex = i
            elif "magnesium" in column:
                self.magnesiumIndex = i
            elif "calcium" in column:
                self.calciumIndex = i
            elif "strontium" in column:
                self.strontiumIndex = i
            elif "fluoride" in column:
                self.fluorideIndex = i
            elif "acetate" in column:
                self.acetateIndex = i
            elif "formate" in column:
                self.formateIndex = i
            elif "chloride" in column:
                self.chlorideIndex = i
            elif "nitrite" in column:
                self.nitriteIndex = i
            elif "bromide" in column:
                self.bromideIndex = i
            elif "nitrate" in column:
                self.nitrateIndex = i
            elif "sulfate" in column:
                self.sulfateIndex = i
            elif "phosphate" == column:
                self.phosphateIndex = i

            i = i + 1
        if self.anionNameIndex == None and self.cationNameIndex == None:
            raise ICMissingNames(self.fileName)

        numNames = 0
        if self.anionNameIndex != None:
            numNames += 1
        if self.cationNameIndex != None:
            numNames += 1

        if len(names) != numNames:
            raise ICMissingNames(self.fileName)

        # quality control for column names
        missings = []
        if self.anionNameIndex != None:
            if self.fluorideIndex == None:
                missings.append("fluoride")
            if self.acetateIndex == None:
                missings.append("acetate")
            if self.formateIndex == None:
                missings.append("formate")
            if self.chlorideIndex == None:
                missings.append("chloride")
            if self.nitriteIndex == None:
                missings.append("nitrite")
            if self.bromideIndex == None:
                missings.append("bromide")
            if self.nitrateIndex == None:
                missings.append("nitrate")
            if self.sulfateIndex == None:
                missings.append("sulfate")
            if self.phosphateIndex == None:
                missings.append("phosphate")
        if self.cationNameIndex != None:
            if self.lithiumIndex == None:
                missings.append("lithium")
            if self.sodiumIndex == None:
                missings.append("sodium")
            if self.ammoniumIndex == None:
                missings.append("ammonium")
            if self.potassiumIndex == None:
                missings.append("pottasium")
            if self.magnesiumIndex == None:
                missings.append("magnesium")
            if self.calciumIndex == None:
                missings.append("calcium")
            if self.strontiumIndex == None:
                missings.append("strontium")
        if len(missings) > 0:
            message = "ERROR: The following expected columns were missing from the headers of the IC sheet: " + str(missings) + "\n\n"
            raise Warnings(message, self.fileName)

    def fixDate(self, date):
        date = date.replace(" ", "") # remove any extra space
        newDate = ""
        for c in date: # change the delimiter from anything else to "-"
            if not c.isalnum():
                newDate = newDate + "-"
            else:
                newDate = newDate + c

        newDateList = newDate.split("-")
        month = newDateList[0]
        day = newDateList[1]
        year = newDateList[2]
        if len(day) == 1:
            day = str(0) + str(day)
        if len(month) == 1:
            month = str(0) + str(month)
        newDate = year + "-" + month + "-" + day

        return newDate

    def fixTime(self, time, meridian):
        time = time.replace(" ", "")
        if "pm" in meridian.lower():
            hour,minute,second = time.split(":")
            hour = int(hour) + 12
            time = str(hour) + ":" + minute + ":" + second
        return time

    def replaceNA(self, string):
        if str(string).lower() == "n.a.":
            return None
        elif str(string).lower() == "nan":
            return None
        else:
            return string

    def isNan(self, num):
        return num == "nan"

    def checkIfAnionsPresent(self):
        if self.acetate == None:
            if self.fluoride == None:
                if self.chloride == None:
                    if self.nitrite == None:
                        if self.bromide == None:
                            if self.nitrate == None:
                                if self.sulfate == None:
                                    if self.phosphate == None:
                                       return False
        return True

    def checkIfCationsPresent(self):
        if self.lithium == None:
            if self.sodium == None:
                if self.ammonium == None:
                    if self.potassium == None:
                        if self.magnesium == None:
                            if self.calcium == None:
                                if self.strontium == None:
                                    return False
        return True

    def resetValues(self):
        self.fluoride = None
        self.acetate = None
        self.formate = None
        self.chloride = None
        self.nitrite = None
        self.bromide = None
        self.nitrate = None
        self.sulfate = None
        self.phosphate = None

        self.lithium = None
        self.sodium = None
        self.ammonium = None
        self.potassium = None
        self.magnesium = None
        self.calcium = None
        self.strontium = None


    def readRow(self, row):
        self.resetValues()

        if self.anionNameIndex != None and self.replaceNA(str(row[self.anionNameIndex])) != None:
            self.anionSortChem = str(row[self.anionNameIndex])

            if self.fluorideIndex != None:
                self.fluoride = self.replaceNA(row[self.fluorideIndex])
            if self.acetateIndex != None:
                self.acetate = self.replaceNA(row[self.acetateIndex])
            if self.formateIndex != None:
                self.formate = self.replaceNA(row[self.formateIndex])
            if self.chlorideIndex != None:
                self.chloride = self.replaceNA(row[self.chlorideIndex])
            if self.nitriteIndex != None:
                self.nitrite = self.replaceNA(row[self.nitriteIndex])
            if self.bromideIndex != None:
                self.bromide = self.replaceNA(row[self.bromideIndex])
            if self.nitrateIndex != None:
                self.nitrate = self.replaceNA(row[self.nitrateIndex])
            if self.sulfateIndex != None:
                self.sulfate = self.replaceNA(row[self.sulfateIndex])
            if self.phosphateIndex != None:
                self.phosphate = self.replaceNA(row[self.phosphateIndex])

            self.rowContainsAnion = self.checkIfAnionsPresent()
        else:
            self.rowContainsAnion = False

        if self.cationNameIndex != None and self.replaceNA(str(row[self.cationNameIndex])) != None:
            self.cationSortChem = str(row[self.cationNameIndex])

            if self.lithiumIndex != None:
                self.lithium = self.replaceNA(row[self.lithiumIndex])
            if self.sodiumIndex != None:
                self.sodium = self.replaceNA(row[self.sodiumIndex])
            if self.ammoniumIndex != None:
                self.ammonium = self.replaceNA(row[self.ammoniumIndex])
            if self.potassiumIndex != None:
                self.potassium = self.replaceNA(row[self.potassiumIndex])
            if self.magnesiumIndex != None:
                self.magnesium = self.replaceNA(row[self.magnesiumIndex])
            if self.calciumIndex != None:
                self.calcium = self.replaceNA(row[self.calciumIndex])
            if self.strontiumIndex != None:
                self.strontium = self.replaceNA(row[self.strontiumIndex])

            self.rowContainsCation = self.checkIfCationsPresent()
        else:
            self.rowContainsCation = False

