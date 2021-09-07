import csv
import traceback

from CustomErrors import batchAlreadyUploadedError, DuplicateNotAllowed
import re

class UploaderEureka:
    def __init__(self, cursor, uploader, eurekaReader):
        self.cursor = cursor
        self.uploader = uploader
        self.eurekaReader = eurekaReader

    def getProjectId(self):
        return self.uploader.getProjectId()

    def uploadEureka(self):
        # parse the eureka info
        self.eurekaReader.readEurekaInfo()

        # check if the eureka exists on the database
        sqlChecker = "SELECT eureka_serial_num FROM eurekas WHERE eureka_serial_num = ?"
        checkerTuple = (self.eurekaReader.serialNum,)

        self.cursor.execute(sqlChecker, checkerTuple)
        result = self.cursor.fetchall()

        if len(result) == 0: # if the eureka is not already in the database

            # upload the eureka to the database
            sqlUploadEureka = "INSERT INTO eurekas (eureka_name, eureka_make, eureka_serial_num) VALUES (?,?,?)"
            uploadEurekaTuple = (self.eurekaReader.name, self.eurekaReader.make, self.eurekaReader.serialNum)

            self.cursor.execute(sqlUploadEureka, uploadEurekaTuple)


    def uploadBatch(self):
        # parse the eureka batch info
        self.eurekaReader.readBatchInfo()

        # check for duplicates
        sqlCheck = "SELECT eureka_serial_num, site_id, date_extracted, first_logging_date, first_logging_time " \
                   "FROM eureka_batches WHERE eureka_serial_num = ? AND site_id = ? and date_extracted = ?" \
                   "and first_logging_date = ? AND first_logging_time = ?"
        checkTuple = (self.eurekaReader.serialNum, self.eurekaReader.siteId, self.eurekaReader.dateExtracted,
                      self.eurekaReader.firstLoggingDate, self.eurekaReader.firstLoggingTime)
        self.cursor.execute(sqlCheck, checkTuple)
        batches = self.cursor.fetchall()

        if not self.uploader.allowDuplicates:
            if len(batches) > 0:
                # raise an error
                raise DuplicateNotAllowed(self.eurekaReader.getFileName())

        sqlBatch = "INSERT INTO eureka_batches (eureka_serial_num, site_id, " \
                   "date_extracted, first_logging_date, first_logging_time, " \
                   "project_id, file_name, file_path) VALUES (?,?,?,?,?,?,?,?)"
        batchTuple = (self.eurekaReader.serialNum, self.eurekaReader.siteId,
                      self.eurekaReader.dateExtracted, self.eurekaReader.firstLoggingDate,
                      self.eurekaReader.firstLoggingTime, self.getProjectId(), self.eurekaReader.fileName, self.eurekaReader.filePath)
        try:
            self.cursor.execute(sqlBatch, batchTuple)
        except:
            raise batchAlreadyUploadedError(self.eurekaReader.getFileName())


        # save the batch info in a member variable
        sqlGetBatchId = "SELECT eureka_batch_id FROM eureka_batches WHERE eureka_serial_num = ? AND site_id = ? AND date_extracted = ?"
        getBatchIdTuple = (self.eurekaReader.serialNum, self.eurekaReader.siteId, self.eurekaReader.dateExtracted)

        self.cursor.execute(sqlGetBatchId, getBatchIdTuple)
        ids = self.cursor.fetchall()
        self.currentBatch = ids[-1][0]

    def uploadLogs(self):
        # parse the individual logs
        with open(self.eurekaReader.getPath(), encoding="utf8") as csvFile:
            reader = csv.reader(csvFile, delimiter=",")

            try:
                for row in reader:
                    if re.match(r"^\d+/\d+/\d+", row[0]): # keep only those rows that start with a date
                        self.eurekaReader.readRow(row)
                        sqlUploadRow = "INSERT INTO eureka_logs (logging_date, " \
                                       "logging_time, temp, ph_units, orp, sp_cond," \
                                       "turbidity, hdo_perc_sat, hdo_concentration, " \
                                       "ph_mv, int_batt_v, eureka_batch_id) VALUES " \
                                       "(?,?,?,?,?,?,?,?,?,?,?,?)"
                        uploadRowTuple = (self.eurekaReader.loggingDate, self.eurekaReader.loggingTime,
                                          self.eurekaReader.temp, self.eurekaReader.phUnits, self.eurekaReader.orp,
                                          self.eurekaReader.spCond, self.eurekaReader.turbidity, self.eurekaReader.hdoPercSat,
                                          self.eurekaReader.hdoConcentration, self.eurekaReader.phMv,
                                          self.eurekaReader.intBattV, self.currentBatch)

                        self.cursor.execute(sqlUploadRow, uploadRowTuple)
            except:
                print(traceback.format_exc())

