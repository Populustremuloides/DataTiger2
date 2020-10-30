import re
import csv
from CustomErrors import NoDataToParse, HoboSerialNumUnparsable, HoboIncorrectlyFormated, HoboMissingData, SiteNotInFileName
from UnitConversions import *
import numpy as np

class ReadHobo():
    def __init__(self, path):
        self.filePath = path
        cleanPath = self.filePath.replace("\\", "/")
        cleanPath = cleanPath.replace("//", "/")
        pathList = cleanPath.split("/")
        self.fileName = pathList[-1]

        self.siteId = None
        self.projectId = None
        self.serialNum = None
        self.downloadDate = None
        self.batchIdentifier = None
        self.dateExtracted = None

        self.firstLoggedDate = None
        self.firstLoggedTime = None

        self.tempIndex = None
        self.dataIndex = None

        self.tempConversion = None
        self.dataConversion = None

    def getFileName(self):
        return self.fileName

    def getFilePath(self):
        return self.filePath

    def getHeaderIndices(self):
        pass
        return None

    def fixDate(self, date):

        ''' out format: mo-dy-yr'''
        a,b,c = date.split("-")

        if self.letterToTypeDict["A"] == "day":
            day = a
        elif self.letterToTypeDict["A"] == "month":
            month = a
        elif self.letterToTypeDict["A"] == "year":
            year = a

        if self.letterToTypeDict["B"] == "day":
            day = b
        elif self.letterToTypeDict["B"] == "month":
            month = b
        elif self.letterToTypeDict["B"] == "year":
            year = b

        if self.letterToTypeDict["C"] == "day":
            day = c
        elif self.letterToTypeDict["C"] == "month":
            month = c
        elif self.letterToTypeDict["C"] == "year":
            year = c

        if len(year) == 4:
            year = year[2:]

        # year = ""
        # month = ""
        # day = ""
        #
        # if len(a) == 4:
        #     # definitely year, month, day
        #     year = a[:2]
        #     month = b
        #     day = c
        # elif len(b) == 4:
        #     pass
        #     # um, weird?
        # elif len(c) == 4:
        #     # either day, month, year or month, day, year
        #     month = a # most commonly seen version in the data
        #     day = b
        #     year = c[:2]
        #
        #     if int(month) > 12: # this rules out the most common answer
        #         day = a
        #         month = b
        #         year = c[:2]
        # else:
        #     # either day, month, year or month, day, year
        #     month = a
        #     day = b
        #     year = c
        #
        #     if int(month) > 12: # our best guess
        #         day = a
        #         month = b
        #         year = c
        #
        #     # SOMETIMES IT IS YEAR MONTH DAY
        #     # go through other hobo files to see if this ever happens
        return month + "-" + day + "-" + year

    def getDateAndTime(self, string):
        stringList = string.split(" ")
        date = stringList[0].replace("/", "-")
        date = self.fixDate(date)
        # print(date)

        time = stringList[1]

        if len(stringList) > 2:
            meridian = stringList[2]

            if meridian == "PM":
                timeList = time.split(":")
                hour = int(timeList[0])
                if hour > 12:
                    hour = hour + 12
                    timeList[0] = str(hour)
                time = ":".join(timeList)

        return[date, time]

    def readHobo(self):
        # open the file
        with open(self.filePath) as csvFile:
            reader = csv.reader(csvFile, delimiter=",")

            try:
                row1 = next(reader)
                row2 = next(reader)
            except:
                raise NoDataToParse(self.filePath)
            try:
                if row1 == None:
                    row1 = ""
                elif type(row1) == type([]):
                    row1 = row1[0]

                row1 = row1.replace(" ","")
                row1 = row1.lower()
                if "serial" in row1:
                    fluff, num = row1.split(":")
                    num = num.replace("-","")
                    num = num.replace(" ","")
                    self.serialNum = num
                else:
                    result = re.search("S/N: \d+", row2[2])
                    snString = result.group(0)
                    snList = snString.split(" ")
                    self.serialNum = snList[1]
                    csvFile.close()
            except:
                csvFile.close()
                raise HoboSerialNumUnparsable(self.filePath)

    def getDataConversion(self, header):
        header = header.replace(" ","")
        header = header.lower()
        if "mmhg" in header:
            return identity
        if "kpa" in header:
            return kpaTommHg
        if "lux" in header:
            return luxToLumens
        if "lum" in header and "ft" in header:
            return identity
        if "mm" in header and "doconc" in header and "l" in header:
            return identity
        if ("%" in header or "perc" in header) and "do" in header:
            return doPercentTomgL
        if "ppm" in header:
            return ppmTomgL
        return identity

    def getTempConversion(self, val):
        val = val.replace(" ","")
        val = val.lower()
        if "f)" in val or "f(" in val:
            return farenheitToCelcius
        elif "c(" in val or "c)" in val:
            return identity

    def getHeaderIndices(self, secondRow):
        for i in range(len(secondRow)):
            val = secondRow[i]
            val = val.lower()
            if "temp" in val:
                self.tempIndex = i
                self.tempConversion = self.getTempConversion(val)
            if "temp" not in val and ("range" in val or "conc" in val or "intensity" in val or "pres" in val or "abs" in val):
                self.dataIndex = i
                self.dataConversion = self.getDataConversion(val)
            if "date" in val and "time" in val:
                self.dateIndex = i
                # FIXE: add a date conversion formula!

    def senseDateFormat(self, dates):

        if len(dates) < 100:
            self.letterToTypeDict = {
                "A": "month",
                "B": "day",
                "C": "year"
            }

        aList = []
        bList = []
        cList = []
        for i in range(len(dates)):
            dates[i] = dates[i].split(" ")[0]
            dates[i] = dates[i].replace("/","-")
            dates[i] = dates[i].replace("\\", "-")

            a, b, c =  dates[i].split("-")

            aList.append(int(a))
            bList.append(int(b))
            cList.append(int(c))

        minA = np.min(aList)
        maxA = np.max(aList)
        rangeA = maxA - minA

        minB = np.min(bList)
        maxB = np.max(bList)
        rangeB = maxB - minB

        minC = np.min(cList)
        maxC = np.max(cList)
        rangeC = maxC - minC

        self.letterToTypeDict = {
            "A":"",
            "B":"",
            "C":""
        }
        letters = ["A","B","C"]
        mins = [minA, minB, minC]
        maxs = [maxA, maxB, maxC]
        ranges = [rangeA, rangeB, rangeC]
        self.letterToTypeDict[letters[np.argmax(ranges)]] = "day"

        # if there are two minimums
        minimums = []
        for j in range(len(ranges)):
            if ranges[j] == min(ranges):
                minimums.append(j)
        if len(minimums) == 2:
            for index in minimums:
                if mins[index] > 12: # the year is the one with the higher values
                    self.letterToTypeDict[letters[index]] = "year"
                if maxs[index] <= 12: # and the month is the one with the lower values
                    self.letterToTypeDict[letters[index]] = "month"
        else:
            # the smallest one is the year
            self.letterToTypeDict[letters[np.argmin(ranges)]] = "year"
            # and the unused one is the month
            for letter in self.letterToTypeDict.keys():
                if self.letterToTypeDict[letter] == "":
                    self.letterToTypeDict[letter] = "month"

        # minimum range and > 12: year
        # maximum range: day
        # medium range: month
        # A, B, C
        print(self.letterToTypeDict)

    def readBatch(self):
        # get the site id from the file name
        fileList = self.fileName.split(".")
        fileWords = fileList[0]
        result = re.search(r"[a-zA-Z]+", fileWords)
        try:
            self.siteId = result.group(0)
        except:
            raise SiteNotInFileName(self.fileName)

        if self.serialNum == None:
            self.readHobo()

        # extract the date if it is in the title
        rawExtractionDate = fileWords.replace(self.serialNum, "")
        rawExtractionDate = rawExtractionDate.replace(self.siteId, "")
        self.extractionDate = ""
        for c in rawExtractionDate:
            if c.isnumeric():
                self.extractionDate = self.extractionDate + c

        if len(self.extractionDate) == 6: # if it is likely to be a date
            year = self.extractionDate[0:2]
            month = self.extractionDate[2:4]
            day = self.extractionDate[4:6]
            self.extractionDate = "20" + year + "-" + month + "-" + day
        else:
            self.extractionDate = None

        # open the file
        with open(self.filePath) as csvFile:
            reader = csv.reader(csvFile, delimiter=",")
            try:
                useThirdRow = True
                useFourthRow = False
                indexToUse = 1

                firsRow = next(reader)
                secondRow = next(reader)
                thirdRow = next(reader)
                fourthRow = next(reader)

                for item in secondRow:
                    if not item.isnumeric():
                        if "#" in item or "date" in item.lower():
                            useThirdRow = True
                            useFourthRow = False
                            if "#" in secondRow[0]:
                                indexToUse = 1
                            else:
                                indexToUse = 0
                for item in thirdRow:
                    if not item.isnumeric():
                        if "#" in item or "date" in item.lower():
                            useThirdRow = False
                            useFourthRow = True
                            if "#" in thirdRow[0]:
                                indexToUse = 1
                            else:
                                indexToUse = 0
                # the header may have "date" or "#" in it
                # the third row may either be a data row, or it may be the header
                # the header may have a #, if it does, then use index 1
                # if it doesn't have a #, then use the index 0
            except:
                raise NoDataToParse(self.fileName)
            # try:

            dates = []
            for row in reader:
                dates.append(row[indexToUse])
            #print(dates)
            self.senseDateFormat(dates)
            # print("happy")
            if useThirdRow:
                self.getHeaderIndices(secondRow)
                self.firstLoggedDate, self.firstLoggedTime = self.getDateAndTime(thirdRow[indexToUse])
            elif useFourthRow:
                self.getHeaderIndices(thirdRow)
                self.firstLoggedDate, self.firstLoggedTime = self.getDateAndTime(fourthRow[indexToUse])


            # except:
            #     raise HoboIncorrectlyFormated(self.fileName)

        self.siteId = self.siteId.upper() # this must be done here because if it were to be done earlier, the date would be messed up


    def readRow(self, row, i):
        # try:
            self.logDate, self.logTime = self.getDateAndTime(row[self.dateIndex])
            self.data = self.dataConversion(row[self.dataIndex])
            if self.tempIndex != None:
                self.temperature = self.tempConversion(row[self.tempIndex])
            else:
                self.temperature = ""

            if self.logDate == "":
                self.logDate = None
            if self.logTime == "":
                self.logTime = None
            if self.data == "":
                self.data = None
            # if self.temperature == "": # sometimes the temperature isn't there. That's okay
            #     self.temperature = None
        # except:
        #     raise HoboMissingData(self.fileName, i)

