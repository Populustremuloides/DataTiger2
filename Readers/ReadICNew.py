
from CustomErrors import *
import pandas as pd
class ReadICNew():
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

        self.sortChem = None
        self.rowContainsAnion = False
        self.rowContainsCation = False

        self.anionNameIndex = None
        self.cationNameIndex = None

        self.headerLine = None
        self.anion = False
        self.cation = False

        self.data = None

    def readBatch(self):

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

        with open(self.filePath) as file:
            l = 0
            for line in file:
                # print(line)
                line = line.split(",")
                if l == 0:
                    i = 0
                    for item in line:
                        if "name" in item.lower():
                            self.nameIndex = i
                        if "amount" in item.lower():
                            amountStartIndex = i
                            break
                        i = i + 1
                    for i in range(amountStartIndex + 1, len(line)):
                        if line[i] != "":
                            amountStopIndex = i
                            break
                        i = i + 1
                # now locate the line where the other headers are and the line where the data start
                for i in range(len(line)):
                    line[i] = line[i].lower()
                if ("lithium" in line) or ("sodium" in line) or ("ammonium" in line) or ("potassium" in line) or (
                        "magnesium" in line) or ("calcium" in line) or ("strontium" in line):
                    self.headerLine = l
                    self.cation = True
                    print("cation header line: " + str(l))

                elif ("fluoride" in line) or ("acetate" in line) or ("formate" in line) or ("chloride" in line) or (
                        "nitrite" in line) or ("sulfate" in line) or ("bromide" in line) or ("nitrate" in line) or (
                        "phosphate" in line):
                    self.headerLine = l
                    print("anion header line: " + str(l))
                    self.anion = True

                if self.headerLine != None:
                    if l == self.headerLine:
                        if self.cation:
                            for i in range(amountStartIndex, amountStopIndex):
                                # get the index of each
                                if "lithium" in line[i].lower():
                                    self.lithiumIndex = i
                                elif "sodium" in line[i].lower():
                                    self.sodiumIndex = i
                                elif "ammonium" in line[i].lower():
                                    self.ammoniumIndex = i
                                elif "potassium" in line[i].lower():
                                    self.potassiumIndex = i
                                elif "magnesium" in line[i].lower():
                                    self.magnesiumIndex = i
                                elif "calcium" in line[i].lower():
                                    self.calciumIndex = i
                                elif "strontium" in line[i].lower():
                                    self.strontiumIndex = i

                        if self.anion:
                            for i in range(amountStartIndex, amountStopIndex):
                                if "fluoride" in line[i].lower():
                                    self.fluorideIndex = i
                                elif "acetate" in line[i].lower():
                                    self.acetateIndex = i
                                elif "formate" in line[i].lower():
                                    self.formateIndex = i
                                elif "chloride" in line[i].lower():
                                    self.chlorideIndex = i
                                elif "nitrite" in line[i].lower():
                                    self.nitriteIndex = i
                                elif "bromide" in line[i].lower():
                                    self.bromideIndex = i
                                elif "nitrate" in line[i].lower():
                                    self.nitrateIndex = i
                                elif "sulfate" in line[i].lower():
                                    self.sulfateIndex = i
                                elif "phosphate" == line[i].lower():
                                    self.phosphateIndex = i
                        break
                l = l + 1

        dataDict = {}
        # make a data dictionary
        if self.nameIndex != None:
            dataDict["sort_chem"] = []
        if self.lithiumIndex != None:
            dataDict["lithium"] = []
        if self.sodiumIndex != None:
            dataDict["sodium"] = []
        if self.ammoniumIndex != None:
            dataDict["ammonium"] = []
        if self.potassiumIndex != None:
            dataDict["potassium"] = []
        if self.magnesiumIndex != None:
            dataDict["magnesium"] = []
        if self.calciumIndex != None:
            dataDict["calcium"] = []
        if self.strontiumIndex != None:
            dataDict["strontium"] = []

        if self.fluorideIndex != None:
            dataDict["fluoride"] = []
        if self.acetateIndex != None:
            dataDict["acetate"] = []
        if self.formateIndex != None:
            dataDict["formate"] = []
        if self.chlorideIndex != None:
            dataDict["chloride"] = []
        if self.nitriteIndex != None:
            dataDict["nitrite"] = []
        if self.bromideIndex != None:
            dataDict["bromide"] = []
        if self.nitrateIndex != None:
            dataDict["nitrate"] = []
        if self.sulfateIndex != None:
            dataDict["sulfate"] = []
        if self.phosphateIndex != None:
            dataDict["phosphate"] = []


        # save all the relevant data in a new dict
        with open(self.filePath) as file:
            l = 0
            for row in file:
                if l > (self.headerLine):
                    row = row.split(",")

                    if self.nameIndex != None:
                        if row[self.nameIndex] != "":
                            dataDict["sort_chem"].append(row[self.nameIndex])
                            # cations
                            if self.lithiumIndex != None:
                                dataDict["lithium"].append(row[self.lithiumIndex])
                            if self.sodiumIndex != None:
                                dataDict["sodium"].append(row[self.sodiumIndex])
                            if self.ammoniumIndex != None:
                                dataDict["ammonium"].append(row[self.ammoniumIndex])
                            if self.potassiumIndex != None:
                                dataDict["potassium"].append(row[self.potassiumIndex])
                            if self.magnesiumIndex != None:
                                dataDict["magnesium"].append(row[self.magnesiumIndex])
                            if self.calciumIndex != None:
                                dataDict["calcium"].append(row[self.calciumIndex])
                            if self.strontiumIndex != None:
                                dataDict["strontium"].append(row[self.strontiumIndex])

                            # anions
                            if self.fluorideIndex != None:
                                dataDict["fluoride"].append(row[self.fluorideIndex])
                            if self.acetateIndex != None:
                                dataDict["acetate"].append(row[self.acetateIndex])
                            if self.formateIndex != None:
                                dataDict["formate"].append(row[self.formateIndex])
                            if self.chlorideIndex != None:
                                dataDict["chloride"].append(row[self.chlorideIndex])
                            if self.nitriteIndex != None:
                                dataDict["nitrite"].append(row[self.nitriteIndex])
                            if self.bromideIndex != None:
                                dataDict["bromide"].append(row[self.bromideIndex])
                            if self.nitrateIndex != None:
                                dataDict["nitrate"].append(row[self.nitrateIndex])
                            if self.sulfateIndex != None:
                                dataDict["sulfate"].append(row[self.sulfateIndex])
                            if self.phosphateIndex != None:
                                dataDict["phosphate"].append(row[self.phosphateIndex])

                l = l + 1

                # locate the name index
                # locate the amount index to the next thing
        self.data = pd.DataFrame.from_dict(dataDict)
        # print(self.data)
        # print("here")


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


    def resetValues(self):
        self.sortChem = None
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

        # print(row)
        if self.nameIndex != None:
            self.sortChem = self.replaceNA(row["sort_chem"])
        if self.fluorideIndex != None:
            self.fluoride = self.replaceNA(row["fluoride"])
        if self.acetateIndex != None:
            self.acetate = self.replaceNA(row["acetate"])
        if self.formateIndex != None:
            self.formate = self.replaceNA(row["formate"])
        if self.chlorideIndex != None:
            self.chloride = self.replaceNA(row["chloride"])
        if self.nitriteIndex != None:
            self.nitrite = self.replaceNA(row["nitrite"])
        if self.bromideIndex != None:
            self.bromide = self.replaceNA(row["bromide"])
        if self.nitrateIndex != None:
            self.nitrate = self.replaceNA(row["nitrate"])
        if self.sulfateIndex != None:
            self.sulfate = self.replaceNA(row["sulfate"])
        if self.phosphateIndex != None:
            self.phosphate = self.replaceNA(row["phosphate"])

        if self.lithiumIndex != None:
            self.lithium = self.replaceNA(row["lithium"])
        if self.sodiumIndex != None:
            self.sodium = self.replaceNA(row["sodium"])
        if self.ammoniumIndex != None:
            self.ammonium = self.replaceNA(row["ammonium"])
        if self.potassiumIndex != None:
            self.potassium = self.replaceNA(row["potassium"])
        if self.magnesiumIndex != None:
            self.magnesium = self.replaceNA(row["magnesium"])
        if self.calciumIndex != None:
            self.calcium = self.replaceNA(row["calcium"])
        if self.strontiumIndex != None:
            self.strontium = self.replaceNA(row["strontium"])
