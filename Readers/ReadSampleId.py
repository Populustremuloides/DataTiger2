from CustomErrors import *
from datetime import datetime

class ReadSampleId:
    def __init__(self, filePath):

        self.missingValues = -1
        self.noZeroOrOne = -2
        self.noError = 0

        self.filePath = filePath

        cleanPath = self.filePath.replace("\\", "/")
        cleanPath = cleanPath.replace("//", "/")
        pathList = cleanPath.split("/")
        self.fileName = pathList[-1]

        self.projectIndex = None
        self.deviceIndex = None
        self.dateIndex = None
        self.siteIndex = None
        self.timeIndex = None
        self.sortChemIndex = None
        self.tempIndex = None
        self.pressIndex = None
        self.o2percentIndex = None
        self.o2mgIndex = None
        self.condIndex = None
        self.phIndex = None
        self.orpIndex = None
        self.chlUGLIndex = None
        self.chlRFUIndex = None
        self.pcUGIndex = None
        self.volumeFilteredIndex = None
        self.calIndex = None
        self.qsaltIndex = None
        self.qtimeIndex = None
        self.notesIndex = None
        self.samplersIndex = None
        self.eventTypeIndex = None
        # self.aqualogIndex = None
        # self.docIndex = None
        # self.elementarIndex = None
        # self.scanIndex = None
        # self.icIndex = None
        # self.icpIndex = None
        # self.lachatIndex = None
        # self.no3Index = None
        # self.srpIndex = None
        # self.waterIndex = None
        # self.ignoreIndex = None

        self.project = None
        self.device = None
        self.date = None
        self.site = None
        self.time = None
        self.sortChem = None
        self.temp = None
        self.press = None
        self.o2percent = None
        self.o2mg = None
        self.cond = None
        self.ph = None
        self.orp = None
        self.chlUGL = None
        self.chlRFU = None
        self.pcUG = None
        self.volumeFiltered = None
        self.cal = None
        self.qsalt = None
        self.qtime = None
        self.notes = None
        self.samplers = None
        self.eventType = None
        self.aqualog = None
        self.doc = None
        self.elementar = None
        self.scan = None
        self.ic = None
        self.icp = None
        self.lachat = None
        self.no3 = None
        self.srp = None
        self.water = None
        self.tss = None
        self.ignore = None

        self.datetimeUploaded = str(datetime.now())

    def resetValues(self):
        self.project = None
        self.device = None
        self.date = None
        self.site = None
        self.time = None
        self.sortChem = None
        self.temp = None
        self.press = None
        self.o2percent = None
        self.o2mg = None
        self.cond = None
        self.ph = None
        self.orp = None
        self.chlUGL = None
        self.chlRFU = None
        self.pcUG = None
        self.volumeFiltered = None
        self.cal = None
        self.qsalt = None
        self.qtime = None
        self.notes = None
        self.samplers = None
        self.eventType = None
        self.aqualog = None
        self.doc = None
        self.elementar = None
        self.scan = None
        self.ic = None
        self.icp = None
        self.lachat = None
        self.no3 = None
        self.srp = None
        self.water = None
        self.tss = None
        self.ignore = None

    def nullIndexPresent(self):
        if self.projectIndex == None:
            return True
        if self.deviceIndex == None:
            return True
        if self.dateIndex == None:
            return True
        if self.siteIndex == None:
            return True
        if self.timeIndex == None:
            return True
        if self.sortChemIndex == None:
            return True
        # if self.tempIndex == None:
        #     return True
        # if self.pressIndex == None:
        #     return True
        # if self.o2percentIndex == None:
        #     return True
        # if self.o2mgIndex == None:
        #     return True
        # if self.condIndex == None:
        #     return True
        # if self.phIndex == None:
        #     return True
        # if self.orpIndex == None:
        #     return True
        # if self.chlUGLIndex == None:
        #     return True
        # if self.chlRFUIndex == None:
        #     return True
        # if self.pcUGIndex == None:
        #     return True
        # if self.volumeFilteredIndex == None:
        #     return True
        # if self.calIndex == None:
        #     return True
        # if self.qsaltIndex == None:
        #     return True
        # if self.qtimeIndex == None:
        #     return True
        # if self.notesIndex == None:
        #     return True
        # if self.samplersIndex == None:
        #     return True
        if self.eventTypeIndex == None:
            return True
        # if self.aqualogIndex == None:
        #     return True
        # if self.docIndex == None:
        #     return True
        # if self.elementarIndex == None:
        #     return True
        # if self.scanIndex == None:
        #     return True
        # if self.icIndex == None:
        #     return True
        # if self.icpIndex == None:
        #     return True
        # if self.lachatIndex == None:
        #     return True
        # if self.no3Index == None:
        #     return True
        # if self.srpIndex == None:
        #     return True
        # if self.waterIndex == None:
        #     return True
        # if self.ignoreIndex == None:
        #     return True

        return False

    def readBatch(self, headers):

        # get the indices of the headers
        i = 0
        for header in headers:
            header = header.lower()
            if "project" in header:
                self.projectIndex = i
            if "device" in header:
                self.deviceIndex = i
            elif "date" in header and "q" not in header:
                self.dateIndex = i
            elif "site" in header:
                self.siteIndex = i
            elif "time" in header and "q" not in header:
                self.timeIndex = i
            elif ("sortchem" in header) or ("sort" in header and "chem" in header):
                self.sortChemIndex = i
            elif "temp" in header:
                self.tempIndex = i
            elif "press" in header:
                self.pressIndex = i
            elif "do%" in header:
                self.o2percentIndex = i
            elif "do" in header and "mg" in header:
                self.o2mgIndex = i
            elif "cond" in header:
                self.condIndex = i
            elif "ph" in header:
                self.phIndex = i
            elif "orp" in header:
                self.orpIndex = i
            elif "chl" in header and "ug" in header:
                self.chlUGLIndex = i
            elif "chl" in header and "rfu" in header:
                self.chlRFUIndex = i
            elif "pc" in header and "ug" in header:
                self.pcUGIndex = i
            elif "volume" in header and "filtered" in header:
                self.volumeFilteredIndex = i
            elif "cal" in header:
                self.calIndex = i
            elif "salt" in header and "g" in header:
                self.qsaltIndex = i
            elif "q" in header and "time" in header:
                self.qtimeIndex = i
            elif "notes" in header:
                self.notesIndex = i
            elif "sampler" in header:
                self.samplersIndex = i
            elif "event" in header and "type" in header:
                self.eventTypeIndex = i
            # elif "aqualog" in header:
            #     self.aqualogIndex = i
            # elif "doci" in header:
            #     self.docIndex = i
            # elif "elementar" in header:
            #     self.elementarIndex = i
            # elif "scan" in header:
            #     self.scanIndex = i
            # elif "ic" in header and not "icp" in header:
            #     self.icIndex = i
            # elif "icp" in header:
            #     self.icpIndex = i
            # elif "lachat" in header:
            #     self.lachatIndex = i
            # elif "no3" in header:
            #     self.no3Index = i
            # elif "srp" in header:
            #     self.srpIndex = i
            # elif "water" in header:
            #     self.waterIndex = i
            # elif "ignore" in header:
            #     self.ignoreIndex = i

            i = i + 1

        if self.nullIndexPresent():
            raise MissingColumn(self.fileName)

    def isnt1or0(self, character):
        if character != "0" and character != "1" and character != 0 and character != 1:
            print(character)
            print(type(character))
            print(character == "1")
            return True
        else:
            return False

    def fixDate(self, date):
        try:
            if "/" in date:
                month, day, year = date.split("/")
                if int(month) > 2000:
                    year, month, day = date.split("/")
            if "-" in date:
                year, month, day = date.split("-")
                if int(day) > 2000:
                    day, month, year = date.split("-")
            return year + "-" + str(month) + "-" + str(day)
        except:
            return "error parsing date"

    def blankForNull(self, string):
        string = str(string)
        if string == "" or string.isspace():
            return None
        else:
            return string

    def criticalNullPresent(self):
        if self.site == None:
            return True
        # if self.project == None:
        #     return True
        if self.sortChem == None:
            return True
        return False

    def replaceWhiteSpace(self):
        self.project = self.blankForNull(self.project)
        self.device = self.blankForNull(self.device)
        self.date = self.blankForNull(self.date)
        self.site = self.blankForNull(self.site)
        self.time = self.blankForNull(self.time)
        self.sortChem = self.blankForNull(self.sortChem)
        self.temp = self.blankForNull(self.temp)
        self.press = self.blankForNull(self.press)
        self.o2percent = self.blankForNull(self.o2percent)
        self.o2mg = self.blankForNull(self.o2mg)
        self.cond = self.blankForNull(self.cond)
        self.ph = self.blankForNull(self.ph)
        self.orp = self.blankForNull(self.orp)
        self.chlUGL = self.blankForNull(self.chlUGL)
        self.chlRFU = self.blankForNull(self.chlRFU)
        self.pcUG = self.blankForNull(self.pcUG)
        self.volumeFiltered = self.blankForNull(self.volumeFiltered)
        self.cal = self.blankForNull(self.cal)
        self.qsalt = self.blankForNull(self.qsalt)
        self.qtime = self.blankForNull(self.qtime)
        self.notes = self.blankForNull(self.notes)
        self.samplers = self.blankForNull(self.samplers)
        self.eventType = self.blankForNull(self.eventType)
        self.aqualog = self.blankForNull(self.aqualog)
        self.doc = self.blankForNull(self.doc)
        self.elementar = self.blankForNull(self.elementar)
        self.scan = self.blankForNull(self.scan)
        self.ic = self.blankForNull(self.ic)
        self.icp = self.blankForNull(self.icp)
        self.lachat = self.blankForNull(self.lachat)
        self.no3 = self.blankForNull(self.no3)
        self.srp = self.blankForNull(self.srp)
        self.water = self.blankForNull(self.water)
        self.tss = self.blankForNull(self.tss)
        self.ignore = self.blankForNull(self.ignore)

    def assignTestValues(self, event):
        if event == "uvu":
            return 1, 0, 1, 1, 1, 1, 1, 0, 1, 0, 1, 0
        elif event == "synoptic":
            return 1, 0, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0
        elif event == "baseline":
            return 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0
        elif "0" in event and "1" in event:
            try:
                return event.strip().split(",")
            except:
                return None, None, None, None, None, None, None, None, None, None, None, None
        elif event == "none":
            return 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
        else:
            return None, None, None, None, None, None, None, None, None, None, None, None
            

    def readRow(self, row):
        self.resetValues()

        # make sure there are enough
        if len(row) < 16:
            return self.missingValues

        if self.projectIndex is not None:
            self.project = row[self.projectIndex]
        if self.deviceIndex is not None:
            self.device = row[self.deviceIndex]
        if self.dateIndex is not None:
            self.date = row[self.dateIndex]
            self.date = self.fixDate(self.date)
        if self.siteIndex is not None:
            self.site = row[self.siteIndex]
            self.site = self.site.replace(" ","")
            try:
                if ("nbs" in self.site.lower()) and ("." in self.site.lower()):
                    nbs, num = self.site.split(".")
                    num = str(int(num))
                    if len(num) == 1:
                        num = "0" + num
                    self.site = "NBS." + num
                elif "nbs" in self.site.lower():
                    self.site = self.site.lower()
                    num = self.site.replace("nbs","")
                    num = str(int(num))
                    if len(num) == 1:
                        num = "0" + num
                    self.site = "NBS." + num
            except:
                self.site = None
        if self.timeIndex is not None:
            self.time = row[self.timeIndex]
        if self.sortChemIndex is not None:
            self.sortChem = row[self.sortChemIndex]
            if self.sortChem == "2021-0301":
                print("stop here!!!")
        if self.tempIndex is not None:
            self.temp = row[self.tempIndex]
        if self.pressIndex is not None:
            self.press = row[self.pressIndex]
        if self.o2percentIndex is not None:
            self.o2percent = row[self.o2percentIndex]
        if self.o2mgIndex is not None:
            self.o2mg = row[self.o2mgIndex]
        if self.condIndex is not None:
            self.cond = row[self.condIndex]
        if self.phIndex is not None:
            self.ph = row[self.phIndex]
        if self.orpIndex is not None:
            self.orp = row[self.orpIndex]
        if self.chlUGLIndex is not None:
            self.chlUGL = row[self.chlUGLIndex]
        if self.chlRFUIndex is not None:
            self.chlRFU = row[self.chlRFUIndex]
        if self.pcUGIndex is not None:
            self.pcUG = row[self.pcUGIndex]
        if self.volumeFilteredIndex is not None:
            self.volumeFiltered = row[self.volumeFilteredIndex]
        if self.calIndex is not None:
            self.cal = row[self.calIndex]
        if self.qsaltIndex is not None:
            self.qsalt = row[self.qsaltIndex]
        if self.qtimeIndex is not None:
            self.qtime = row[self.qtimeIndex]
        if self.notesIndex is not None:
            self.notes = row[self.notesIndex]
        if self.samplersIndex is not None:
            self.samplers = row[self.samplersIndex]
        if self.eventTypeIndex is not None:
            self.eventType = row[self.eventTypeIndex].lower()
            self.aqualog, self.doc, self.elementar, self.scan, self.ic, self.icp, self.lachat, self.no3, self.srp, self.water, self.tss, self.ignore = self.assignTestValues(self.eventType)      # if self.aqualogIndex is not None:
        #     self.aqualog = row[self.aqualogIndex]
        # if self.docIndex is not None:
        #     self.doc = row[self.docIndex]
        # if self.elementarIndex is not None:
        #     self.elementar = row[self.elementarIndex]
        # if self.scanIndex is not None:
        #     self.scan = row[self.scanIndex]
        # if self.icIndex is not None:
        #     self.ic = row[self.icIndex]
        # if self.icpIndex is not None:
        #     self.icp = row[self.icpIndex]
        # if self.lachatIndex is not None:
        #     self.lachat = row[self.lachatIndex]
        # if self.no3Index is not None:
        #     self.no3 = row[self.no3Index]
        # if self.srpIndex is not None:
        #     self.srp = row[self.srpIndex]
        # if self.waterIndex is not None:
        #     self.water = row[self.waterIndex]
        # if self.ignoreIndex is not None:
        #     self.ignore = row[self.ignoreIndex]

        # make sure there were 0's and 1's
        if self.aqualog == "":
            print("look here")
        if self.isnt1or0(self.aqualog):
            print('aqualog')
            return self.noZeroOrOne
        if self.isnt1or0(self.doc):
            print("doc")
            return self.noZeroOrOne
        if self.isnt1or0(self.elementar):
            print("elementar")
            return self.noZeroOrOne
        if self.isnt1or0(self.scan):
            print("scan")
            return self.noZeroOrOne
        if self.isnt1or0(self.ic):
            print("ic")
            return self.noZeroOrOne
        if self.isnt1or0(self.icp):
            print('icp')
            return self.noZeroOrOne
        if self.isnt1or0(self.lachat):
            print("lachat")
            return self.noZeroOrOne
        if self.isnt1or0(self.no3):
            print("no3")
            return self.noZeroOrOne
        if self.isnt1or0(self.srp):
            print("srp")
            return self.noZeroOrOne
        if self.isnt1or0(self.water):
            print('water')
            return self.noZeroOrOne
        if self.isnt1or0(self.ignore):
            print("ignore")
            return self.noZeroOrOne

        self.replaceWhiteSpace()

        if self.criticalNullPresent():
            return self.missingValues
        else:
            return self.noError


