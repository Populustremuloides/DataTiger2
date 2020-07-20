from CustomErrors import *

class ReadScanPAR():
    def __init__(self, filePath):
        self.filePath = filePath
        cleanPath = self.filePath.replace("\\", "/")
        cleanPath = cleanPath.replace("//", "/")
        pathList = cleanPath.split("/")
        self.fileName = pathList[-1]

    def resetValues(self):
        self.datetimeValue = None
        self.turbidity = None
        self.noc3 = None
        self.toc = None
        self.doc = None

    def readRow(self, row):
        self.resetValues()

        self.datetimeValue = row[self.datetimeIndex]
        self.datetimeValue = self.datetimeValue.replace(".","-")

        self.turbidity = row[self.turbidityIndex]
        self.noc3 = row[self.noc3Index]
        self.toc = row[self.tocIndex]
        self.doc = row[self.docIndex]


    def readBatch(self, header, columns):



        # grab the serial number of the machine that ran the test
        if "Timestamp" in header[0]:
            if len(header) < 3:
                raise FileTypeNotRecognized(self.fileName)
            self.spectrolyzer = header[3]
        else:
            self.spectrolyzer = header[0].split("_")[0]
        newSpec = ""
        for c in self.spectrolyzer:
            if c.isnumeric():
                newSpec = newSpec + c
        self.spectrolyzer = newSpec

        i = 0
        for column in columns:
            if "Date/Time" in column:
                self.datetimeIndex = i
            elif "Measurement interval" in column:
                self.datetimeIndex = i
            elif "Turbid. " in column:
                self.turbidityIndex = i
            elif ("Turbidity" in column) and ("Status" not in column):
                self.turbidityIndex = i
            elif "NO3-Neq " in column and not ("Status" in column):
                self.noc3Index = i
            elif "NO3eq" in column and not ("Status" in column):
                self.noc3Index = i
            elif "TOCeq " in column and not ("Status" in column):
                self.tocIndex = i
            elif "DOCeq " in column and not ("Status" in column):
                self.docIndex = i
            i = i + 1

