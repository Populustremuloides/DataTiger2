import pandas as pd
from CustomErrors import *
from Readers.ReadQ import *
from ComputeQ import *

# go through all the rows
# for each remark, append to a dictioary[remark].append(conductivity)
# for each key in the dictionary, do a q operation with the Qreader

class UploadYSI:
    def __init__(self, cursor, uploader, ysiReader):
        self.cursor = cursor
        self.uploader = uploader
        self.ysiReader = ysiReader
        self.batchNumber = None

        self.qComputer = ComputeQ()

        self.noErrors = 0
        self.error = 1

        self.remarksToEc = {}
        self.remarksToIntervals = {}
        self.remarksToQList = {}


        self.duplicateQs = []
        self.problemQs = []

    def duplicateBatch(self):
        # see if the file name and project have already been uploaded
        sqlBatch = "SELECT * FROM q_batches WHERE file_name = ?;"
        batchTuple = (self.ysiReader.fileName,)

        self.cursor.execute(sqlBatch, batchTuple)
        result = self.cursor.fetchall()

        if len(result) > 0:
            return True
        else:
            return False

    def uploadBatchHelper(self):
        sqlBatch = "INSERT INTO q_batches " \
                   "(project_id, file_name, file_path, datetime_uploaded) VALUES (?,?,?,?);"
        batchTuple = (self.uploader.getProjectId(), self.ysiReader.fileName,
                      self.ysiReader.filePath, self.ysiReader.datetimeUploaded)

        self.cursor.execute(sqlBatch, batchTuple)

    def getBatchNumber(self):
        sqlBatch = "SELECT q_batch_id FROM q_batches WHERE " \
                   "datetime_uploaded = ?;"
        batchTuple = (self.ysiReader.datetimeUploaded,)

        self.cursor.execute(sqlBatch, batchTuple)
        result = self.cursor.fetchall()[0][0]
        return result

    def uploadBatch(self):
        # check for duplicates
        if self.duplicateBatch() and not self.uploader.allowDuplicates:
            raise DuplicateBatch(self.ysiReader.fileName)
        else:
            # get the headers

            with open(self.ysiReader.filePath, "r+", encoding='utf-16-le') as dataFile:
                for i in range(15):
                    fluff = dataFile.readline()
                headers = dataFile.readline()
                headers = headers.replace("\n","")
                headers = headers.split(",")
            self.ysiReader.readBatch(headers)

            self.uploadBatchHelper()
            self.batchNumber = self.getBatchNumber()

    def duplicateQ(self, q):
        qList = self.qComputer.remarksToQList[q]
        try:
            siteId = qList[0]
            dateSampled = qList[1]
            timeSampled = qList[2]

        except:
            return False

        sqlDuplicate = "SELECT * FROM q_reads WHERE date_sampled = ? AND time_sampled = ? AND site_id = ?;"
        duplicateTuple = (str(dateSampled), str(timeSampled), siteId)
        self.cursor.execute(sqlDuplicate, duplicateTuple)

        result = self.cursor.fetchall()
        if len(result) > 0:
            return True
        else:
            return False

    def uploadQ(self, q):
        qList = self.qComputer.remarksToQList[q]
        try:
            siteId = qList[0]
            dateSampled = qList[1]
            timeSampled = qList[2]
            discharge = qList[3]


        except:
            message = "ERROR: unable to parse the q file correctly because it was not formatted correctly."
            path = self.ysiReader.fileName
            raise BlankError(message, path)

        sqlQ = "INSERT INTO q_reads (q_batch_id, site_id, date_sampled, time_sampled, discharge) VALUES (?,?,?,?,?);"
        qTuple = (self.batchNumber, siteId, dateSampled, timeSampled, discharge)

        self.cursor.execute(sqlQ, qTuple)


    def parseReads(self):
        self.problemRows = []

        previousTime = None
        with open(self.ysiReader.filePath, "r+", encoding='utf-16-le') as dataFile:
            for i in range(15):
                fluff = dataFile.readline()
                fluff = fluff.replace("\n","")
                fluff = fluff.split(",")
                if i == 3:
                    site = fluff[1]
                if i == 4:
                    saltG = fluff[1]
            remark = site + "-" + saltG
            headers = dataFile.readline()
            for line in dataFile:
                line = line.replace("\n","")
                line = line.split(",")
                result = self.ysiReader.readRow(list(line))

                if result != self.ysiReader.noErrors:
                    self.problemRows.append(index + 2)
                else:
                    # record the starting time + location:
                    if not remark in self.remarksToQList.keys():
                        print("POPULATING Q-LIST")
                        print(site)
                        print(self.ysiReader.date)
                        print(self.ysiReader.time)
                        self.remarksToQList[remark] = [site, self.ysiReader.date, self.ysiReader.time]

                    # record the intervals
                    if previousTime != None:
                        interval = int(self.ysiReader.time.split(":")[-1]) - int(previousTime.split(":")[-1])
                        if interval < 0:
                            interval = interval + 60

                        if not remark in self.remarksToIntervals:
                            self.remarksToIntervals[remark] = []
                        self.remarksToIntervals[remark].append(interval)

                    previousTime = self.ysiReader.time

                    if not remark in self.remarksToEc.keys():
                        self.remarksToEc[remark] = []

                    self.remarksToEc[remark].append(self.ysiReader.ec)


    def uploadReads(self):
        self.problemQs = []

        # parse the data
        self.parseReads()

        # perform calculations on the data
        self.remarkstoEc, self.remarksToIntervals, self.remarksToQList = self.qComputer.computeQs(
            self.remarksToEc, self.remarksToIntervals, self.remarksToQList)

        # upload it to the database
        for q in self.qComputer.remarksToQList.keys():
            if q in self.qComputer.problemQs:
                self.problemQs.append(q)
            if self.duplicateQ(q) and not self.uploader.allowDuplicates:
                self.duplicateQs.append(q)
            else:
                try:
                    self.uploadQ(q)
                except:
                    pass

        message = ""
        if len(self.problemRows) > 0:
            message = message + "ERROR: the following rows were likely missing critical values and were therefore skipped " \
                      " when calculating Qs: " + str(self.problemRows) + "\n"
        if len(self.problemQs) > 0:
            message = message + "ERROR: the q value(s) with the remark(s) labeled " + str(self.problemQs) + " were not able to" \
                                " be calculated correctly. Please examine those reads and try again.\n"
        if len(self.duplicateQs) > 0:
            message = message + "ERROR: the q value(s) labeled " + str(self.duplicateQs) + " were not uploaded to the database" \
                                " because they were duplicates of an identical q-remark with the same time signature already present in the database. If you " \
                                "would like to upload these q-values anyway, please click \'allow duplicates\' above and resumbit."
        if message != "":
            raise Warnings(message, self.ysiReader.fileName)

