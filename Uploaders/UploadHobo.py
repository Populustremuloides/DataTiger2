from Readers.ReadHobo import *
from CustomErrors import HoboRowsError, batchAlreadyUploadedError, DuplicateNotAllowed
from UnitConversions import *

class UploadHobo():

    def __init__(self, cursor, uploader, hoboReader, dataName, logTableName, batchTableName):
        self.cursor = cursor
        self.uploader = uploader
        self.hoboReader = hoboReader
        self.dataName = dataName
        self.logTableName = logTableName
        self.batchTableName = batchTableName

    def getProjectId(self):
        return self.uploader.getProjectId()



    def uploadLogs(self):
        problemRows = []
        noErrors = True

        with open(self.hoboReader.getFilePath()) as csvFile:
            reader = csv.reader(csvFile, delimiter=",")
            i = 3
            for row in reader:
                if len(row) > 0:
                    if row[0][0].isnumeric() and i >= 5:
                        # parse the row
                        self.hoboReader.readRow(row, i)

                        # upload the row
                        sqlLog = "INSERT INTO " + self.logTableName + " (logging_date, logging_time, " + self.dataName + ", " \
                                "temperature_celsius, batch_id) VALUES (?,?,?,?,?)"
                        logTuple = (self.hoboReader.logDate, self.hoboReader.logTime, self.hoboReader.data,
                                self.hoboReader.temperature, self.currentBatch)
                        try:
                            self.cursor.execute(sqlLog, logTuple)

                        except:
                            problemRows.append(i)
                            noErrors = False
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
                   "AND first_logged_date = ? AND first_logged_time = ? "
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
                raise DuplicateNotAllowed(self.hoboReader.getFileName())


        # upload to the database
        sqlBatch = "INSERT INTO " + self.batchTableName + " (site_id, project_id, hobo_serial_num, first_logged_date," \
                   "first_logged_time, date_extracted, file_name, file_path) VALUES (?,?,?,?,?,?,?,?)"
        batchTuple = (self.hoboReader.siteId, self.getProjectId(), self.hoboReader.serialNum,
                      self.hoboReader.firstLoggedDate, self.hoboReader.firstLoggedTime,
                      self.hoboReader.extractionDate, self.hoboReader.fileName, self.hoboReader.filePath)
        try:
            self.cursor.execute(sqlBatch, batchTuple)
        except:
            raise batchAlreadyUploadedError(self.hoboReader.getFilePath())

        # get the batch id

        sqlId = "SELECT batch_id FROM " + self.batchTableName + " WHERE project_id = ? AND site_id = ? AND hobo_serial_num = ? AND " \
                "first_logged_date = ? AND first_logged_time = ?"

        idTuple = (self.getProjectId(), self.hoboReader.siteId, self.hoboReader.serialNum, self.hoboReader.firstLoggedDate,
                   self.hoboReader.firstLoggedTime)

        result = self.cursor.execute(sqlId, idTuple)
        ids = self.cursor.fetchall()
        self.currentBatch = ids[-1][0]


    def uploadHobo(self):
        self.hoboReader.readHobo()

        sqlHobo = "SELECT hobo_serial_num FROM hobos WHERE hobo_serial_num = ?"

        hoboTuple = (self.hoboReader.serialNum,)

        result = self.cursor.execute(sqlHobo, hoboTuple)
        hobos = self.cursor.fetchall()

        if len(hobos) < 1:

            sqlHobo = "INSERT INTO hobos (hobo_serial_num) VALUES (?)"
            hoboTuple = (self.hoboReader.serialNum,)

            self.cursor.execute(sqlHobo, hoboTuple)


