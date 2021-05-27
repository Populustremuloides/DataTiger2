import traceback
from Readers.ReadHobo import *
from CustomErrors import HoboRowsError, batchAlreadyUploadedError, DuplicateNotAllowed, BadHobo
from UnitConversions import *
from datetime import datetime

class UploadHobo():

    def __init__(self, cursor, uploader, hoboReader, dataName, logTableName, batchTableName):
        self.cursor = cursor
        self.uploader = uploader
        self.hoboReader = hoboReader
        self.dataName = dataName
        self.logTableName = logTableName
        self.batchTableName = batchTableName
        self.autocorrelationCoefficient = None
        if dataName == "intensity":
            self.autocorrelationThreshold = 1.7
        else:
            self.autocorrelationThreshold = 0.7

    def getProjectId(self):
        # cheat for the moment
        return "Megafire"
        # return self.uploader.getProjectId()

    def autocorrelation(self, list):

        diffs = []
        for i in range(len(list) - 1):
            try:
                a = list[i]
                b = list[i + 1]
                diff = abs(b - a)
                weightedDiff = diff / abs(a)
                diffs.append(weightedDiff)
            except:
                pass
        return np.mean(diffs)

    def uploadLogs(self):
        problemRows = []
        noErrors = True

        print("autocorrelation number: (<<1 is normal for non-light files)")
        print(self.autocorrelationCoefficient)

        if self.autocorrelationCoefficient > self.autocorrelationThreshold:
            raise BadHobo(self.hoboReader.getFileName())

        with open(self.hoboReader.getFilePath()) as csvFile:
            reader = csv.reader(csvFile, delimiter=",")
            i = 3
            for row in reader:
                if len(row) > 0:
                    if row[0][0].isnumeric() and i >= 5:
                        # parse the row
                        if self.hoboReader.readRow(row, i, self.hoboReader.firstLoggedDateTime):
                            # upload the row
                            sqlLog = "INSERT INTO " + self.logTableName + " (logging_date, logging_time, " + self.dataName + ", " \
                                    "temperature_celsius, batch_id) VALUES (?,?,?,?,?)"
                            logTuple = (self.hoboReader.logDate, self.hoboReader.logTime, self.hoboReader.data,
                                    self.hoboReader.temperature, self.currentBatch)
                            # try:
                            self.cursor.execute(sqlLog, logTuple)
                        else:
                            problemRows.append(i)
                        # except:
                        #     problemRows.append(i)
                        #     noErrors = False
                            # raise HoboRowError(self.hoboReader.getFilePath(), i)
                else:
                    problemRows.append(i)
                i = i + 1

        if not noErrors and (len(problemRows) > 2):
            raise HoboRowsError(self.hoboReader.fileName, problemRows)


    def uploadBatch(self):
        print(self.getProjectId())

        # parse or request the necessary info
        self.hoboReader.readBatch()

        # check for duplicates
        sqlCheck = "SELECT * FROM " + self.batchTableName + " WHERE site_id = ? AND project_id = ? AND hobo_serial_num = ? " \
                   "AND first_logged_date = ? AND first_logged_time = ?"
        checkTuple = (
            self.hoboReader.siteId,
            self.getProjectId(),
            self.hoboReader.serialNum,
            self.hoboReader.firstLoggedDate,
            self.hoboReader.firstLoggedTime,
        )

        self.cursor.execute(sqlCheck, checkTuple)
        batches = self.cursor.fetchall()

        if not self.uploader.allowDuplicates:
            if len(batches) > 0:
                # check to see whether the last logged date + time of last batch are less than the last logged date + time of this batch
                for batch in batches:
                    if batch[12] is None:
                        # if old batch is legacy and doesn't have a last_logged_date, just add to database (essentially an automation of checking allow duplicates when this kind of file doesn't go through)
                        pass
                    elif self.hoboReader.lastLoggedDate == batch[12] and self.hoboReader.lastLoggedTime == batch[13]:
                        # first and last logged date + time are the same, don't add
                        raise DuplicateNotAllowed(self.hoboReader.getFileName())
                    else:
                        # clean date and time
                        oldBatchTime = batch[13] + ":00" if len(batch[13].split(":")) < 3 else batch[13]
                        oldBatchLastDate = datetime.strptime(batch[12] + ' ' + oldBatchTime, '%m-%d-%y %H:%M:%S')
                        newBatchTime = self.hoboReader.lastLoggedTime + ":00" if len(self.hoboReader.lastLoggedTime.split(":")) < 3 else self.hoboReader.lastLoggedTime
                        newBatchLastDate = datetime.strptime(self.hoboReader.lastLoggedDate + ' ' + newBatchTime, '%m-%d-%y %H:%M:%S')

                        # if old batch's last logged date is less recent than new batch's, check whether new lines have already been added
                        if oldBatchLastDate < newBatchLastDate:
                            sqlCheck = "SELECT * FROM " + self.batchTableName + " WHERE site_id = ? AND project_id = ? AND hobo_serial_num = ? " \
                                                                                "AND first_logged_date = ? AND first_logged_time = ?"
                            checkTuple = (
                                self.hoboReader.siteId,
                                self.getProjectId(),
                                self.hoboReader.serialNum,
                                batch[12], batch[13]
                            )
                            
                            self.cursor.execute(sqlCheck, checkTuple)
                            secondbatches = self.cursor.fetchall()
                            if len(secondbatches) > 0:
                                raise DuplicateNotAllowed(self.hoboReader.getFileName())
                            else:
                                print(batch[12] + ' ' + oldBatchTime + " < " + self.hoboReader.lastLoggedDate + ' ' + newBatchTime)

                                # if not already added, set first logged date and time to old batch's last logged date and time and add new lines
                                self.hoboReader.firstLoggedDate = batch[12]
                                self.hoboReader.firstLoggedTime = batch[13]

        # test if it is a bad file
        datalist = []
        with open(self.hoboReader.getFilePath()) as csvFile:
            # clean date and time
            batchTime = self.hoboReader.firstLoggedTime + ":00" if len(self.hoboReader.firstLoggedTime.split(":")) < 3 else self.hoboReader.firstLoggedTime
            self.hoboReader.firstLoggedDateTime = datetime.strptime(self.hoboReader.firstLoggedDate + ' ' + batchTime,'%m-%d-%y %H:%M:%S')
            reader = csv.reader(csvFile, delimiter=",")
            i = 3
            for row in reader:
                if len(row) > 0:
                    if row[0][0].isnumeric() and i >= 5:
                        self.hoboReader.readRow(row, i, self.hoboReader.firstLoggedDateTime)
                        try:
                            datalist.append(float(self.hoboReader.data))
                        except:
                            pass
                i = i + 1
        self.autocorrelationCoefficient = self.autocorrelation(datalist)
        goodData = 1
        if self.autocorrelationCoefficient > self.autocorrelationThreshold or self.autocorrelationCoefficient is None:
            goodData = 0

        # upload to the database
        sqlBatch = "INSERT INTO " + self.batchTableName + " (site_id, project_id, hobo_serial_num, first_logged_date," \
                   "first_logged_time, last_logged_date, last_logged_time, date_extracted, file_name, file_path, datetime_uploaded, " \
                                                          "good_data, autocorrelation_value) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"
        batchTuple = (self.hoboReader.siteId, self.getProjectId(), self.hoboReader.serialNum,
                      self.hoboReader.firstLoggedDate, self.hoboReader.firstLoggedTime, self.hoboReader.lastLoggedDate, self.hoboReader.lastLoggedTime,
                      self.hoboReader.extractionDate, self.hoboReader.fileName, self.hoboReader.filePath,
                      self.hoboReader.datetimeUploaded, goodData, self.autocorrelationCoefficient)
        try:
            self.cursor.execute(sqlBatch, batchTuple)
        except:
            print(traceback.format_exc())


        # get the batch id

        sqlId = "SELECT batch_id FROM " + self.batchTableName + " WHERE project_id = ? AND site_id = ? AND hobo_serial_num = ? AND " \
                "first_logged_date = ? AND first_logged_time = ? AND last_logged_date = ? AND last_logged_time = ?"

        idTuple = (self.getProjectId(), self.hoboReader.siteId, self.hoboReader.serialNum, self.hoboReader.firstLoggedDate,
                   self.hoboReader.firstLoggedTime, self.hoboReader.lastLoggedDate, self.hoboReader.lastLoggedTime)

        result = self.cursor.execute(sqlId, idTuple)
        ids = self.cursor.fetchall()
        self.currentBatch = ids[-1][0]


    def uploadHobo(self):
        self.hoboReader.readHobo()

        sqlHobo = "SELECT hobo_serial_num FROM hobos_1 WHERE hobo_serial_num = ?"

        hoboTuple = (self.hoboReader.serialNum,)

        result = self.cursor.execute(sqlHobo, hoboTuple)
        hobos = self.cursor.fetchall()

        if len(hobos) < 1:

            sqlHobo = "INSERT INTO hobos_1 (hobo_serial_num) VALUES (?)"
            hoboTuple = (self.hoboReader.serialNum,)

            self.cursor.execute(sqlHobo, hoboTuple)
