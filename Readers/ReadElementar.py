from CustomErrors import *

class ReadElementar():
    def __init__(self, filePath):
        self.filePath = filePath
        self.sortChem = None
        self.make = None
        self.serialNum = None

        cleanPath = self.filePath.replace("\\", "/")
        cleanPath = cleanPath.replace("//", "/")
        pathList = cleanPath.split("/")
        self.fileName = pathList[-1]

        self.hole = None
        self.sortChem = None
        self.method = None
        self.ticArea = None
        self.tcArea = None
        self.npocArea = None
        self.tnbArea = None
        self.ticArea = None
        self.tcConcentraiton = None
        self.tocConcnetration = None
        self.npocConcentration = None
        self.tnbConcentration = None
        self.datetime = None
        self.date = None
        self.time = None

        self.holeIndex = None
        self.sortChemIndex = None
        self.methodIndex = None
        self.ticAreaIndex = None
        self.tcAreaIndex = None
        self.npocAreaIndex = None
        self.tnbAreaIndex = None
        self.ticConcentrationIndex = None
        self.tcConcentrationIndex = None
        self.tocConcentrationIndex = None
        self.npocConcentrationIndex = None
        self.tnbConcentrationIndex = None
        self.datetimeIndex = None
        self.dateIndex = None
        self.timeIndex = None

        self.timeWeird = False

    def getPath(self):
        return self.filePath

    def getFileName(self):
        return self.fileName

    def readBatch(self, columns):
        # parse the file name        # get the filename
        nameList = self.fileName.split(".")
        if len(nameList) == 6:
            self.runDate = nameList[0] + "-" + nameList[1] + "-" + nameList[2]
            self.projectId = nameList[3]
            self.operator = nameList[4]
        else:
            try:
                self.runDate = nameList[0]
                self.projectId = nameList[1]
                self.operator = nameList[2]

                # fix the dateTime
                year = self.runDate[0:4]
                month = self.runDate[4:6]
                day = self.runDate[6:8]
                self.runDate = year + "-" + month + "-" + day

            except:
                raise ElementarFileNotNamedCorrectly(self.fileName)

        self.projectId = self.projectId.replace(" ","")


        # FIXME: get the column indices of the columns we want to parse
        i = 0
        for column in columns:
            if "Hole" in column:
                self.holeIndex = i
            elif "Name" in column:
                self.sortChemIndex = i
            elif "Method" in column:
                self.methodIndex = i
            elif "TIC" in column and "Area" in column:
                self.ticAreaIndex = i
            elif "TC" in column and "Area" in column:
                self.tcAreaIndex = i
            elif "NPOC" in column and "Area" in column:
                self.npocAreaIndex = i
            elif "TNb" in column and "Area" in column:
                self.tnbAreaIndex = i
            elif "TIC" in column and "[mg/l]" in column:
                self.ticConcentrationIndex = i
            elif "TC" in column and  "[mg/l]" in column:
                self.tcConcentrationIndex = i
            elif "TOC (Diff.) [mg/l]" in column:
                self.tocConcentrationIndex = i
            elif "NPOC" in column and "[mg/l]" in column:
                self.npocConcentrationIndex = i
            elif "TNb" in column and "[mg/l]" in column:
                self.tnbConcentrationIndex = i
            elif "Date" in column and "Time" in column:
                self.datetimeIndex = i
            elif "Date" in column:
                self.dateIndex = i
            elif "Time" in column:
                self.timeIndex = i

            i = i + 1

        if self.nullsInIndices():
            raise ElementarFileIncorrectlyFormated(self.fileName)
        if self.timeIndex == None:
            self.timeWeird = True

    def nullsInIndices(self):
        if self.holeIndex == None:
            return True
        elif self.sortChemIndex == None:
            return True
        elif self.methodIndex == None:
            return True
        elif self.ticAreaIndex == None:
            return True
        elif self.tcAreaIndex == None:
            return True
        elif self.npocAreaIndex == None:
            return True
        elif self.tnbAreaIndex == None:
            return True
        elif self.ticConcentrationIndex == None:
            return True
        elif self.tcConcentrationIndex == None:
            return True
        elif self.tocConcentrationIndex == None:
            return True
        elif self.npocConcentrationIndex == None:
            return True
        elif self.tnbConcentrationIndex == None:
            return True
        elif self.datetimeIndex == None:
            if self.dateIndex == None and self.timeIndex == None:
                return True
        else:
            return False

    def replaceEmptyWithNull(self):
        if self.hole == "" or self.hole.isspace():
            self.hole = None
        if self.sortChem == "" or self.sortChem.isspace():
            self.sortChem = None
        if self.method == "" or self.method.isspace():
            self.method = None
        if self.ticArea == "" or self.ticArea.isspace():
            self.ticArea = None
        if self.tcArea == "" or self.tcArea.isspace():
            self.tcArea = None
        if self.npocArea == "" or self.npocArea.isspace():
            self.npocArea = None
        if self.tnbArea == "" or self.tnbArea.isspace():
            self.tnbArea = None
        if self.ticConcentration == "" or self.ticConcentration.isspace():
            self.ticConcentration = None
        if self.tcConcentration == "" or self.tcConcentration.isspace():
            self.tcConcentration = None
        if self.tocConcentration == "" or self.tocConcentration.isspace():
            self.tocConcentration = None
        if self.npocConcentration == "" or self.npocConcentration.isspace():
            self.npocConcentration = None
        if self.tnbConcentration == "" or self.tnbConcentration.isspace():
            self.tnbConcentration = None
        if self.datetime == "" or self.date.isspace():
            self.datetime = None
        if self.date == "" or self.date.isspace():
            self.date = None
        if self.time == "" or self.time.isspace():
            self.time = None

    def nullInRow(self):
        if self.hole == None:
            return True
        elif self.sortChem == None:
            return True
        elif self.method == None:
            return True
        else:
            return False


    def resetValues(self):
        self.hole = None
        self.sortChem = None
        self.method = None
        self.ticArea = None
        self.tcArea = None
        self.npocArea = None
        self.tnbArea = None
        self.ticArea = None
        self.ticConcentration = None
        self.tcConcentraiton = None
        self.tocConcnetration = None
        self.npocConcentration = None
        self.tnbConcentration = None
        self.date = None
        self.time = None

    def readRow(self, row):
        self.resetValues()

        self.hole = row[self.holeIndex]
        self.sortChem = row[self.sortChemIndex]
        self.method = row[self.methodIndex]
        self.ticArea = row[self.ticAreaIndex]
        self.tcArea = row[self.tcAreaIndex]
        self.npocArea = row[self.npocAreaIndex]
        self.tnbArea = row[self.tnbAreaIndex]
        self.ticArea = row[self.ticConcentrationIndex]
        self.ticConcentration = row[self.ticConcentrationIndex]
        self.tcConcentration = row[self.tcConcentrationIndex]
        self.tocConcentration = row[self.tocConcentrationIndex]
        self.npocConcentration = row[self.npocConcentrationIndex]
        self.tnbConcentration = row[self.tnbConcentrationIndex]

        if self.datetimeIndex == None:
            self.date = row[self.dateIndex]
            self.time = row[self.timeIndex]
        else:
            datetime = row[self.datetimeIndex]
            self.date, self.time = datetime.split(" ")

        # fix the date up a bit. . .
        # self.date, self.time = self.dateTime.split(" ")
        if len(self.date) > 3:
            day, month, year = self.date.split(".")
            self.date = str(year) + "-" + str(month) + "-" + str(day)

        self.replaceEmptyWithNull()

        if self.nullInRow():
            return -1
        else:
            return 0
