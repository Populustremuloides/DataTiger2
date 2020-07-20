import pandas as pd
from CustomErrors import *

class UploadSites:
    def __init__(self, cursor, uploader, sitesReader):
        self.cursor = cursor
        self.uploader = uploader
        self.sitesReader = sitesReader
        self.batchNumber = None

        self.noErrors = 0
        self.error = 1

        # open the file as a pandas dataframe object
        self.df = pd.read_excel(self.sitesReader.filePath)

    def duplicateBatch(self):
        # see if the file name and project have already been uploaded
        sqlBatch = "SELECT * FROM sites_batches WHERE file_name = ?;"
        batchTuple = (self.sitesReader.fileName,)

        self.cursor.execute(sqlBatch, batchTuple)
        result = self.cursor.fetchall()

        if len(result) > 0:
            return True
        else:
            return False

    def uploadBatchHelper(self):
        sqlBatch = "INSERT INTO sites_batches " \
                   "(file_name, file_path, datetime_uploaded) VALUES (?,?,?);"
        batchTuple = (self.sitesReader.fileName, self.sitesReader.filePath, self.sitesReader.datetimeUploaded)

        self.cursor.execute(sqlBatch, batchTuple)

    def getBatchNumber(self):
        sqlBatch = "SELECT sites_batch_id FROM sites_batches WHERE " \
                   "datetime_uploaded = ?;"
        batchTuple = (self.sitesReader.datetimeUploaded,)

        self.cursor.execute(sqlBatch, batchTuple)
        result = self.cursor.fetchall()[0][0]
        return result

    def uploadBatch(self):
        # check for duplicates
        if self.duplicateBatch() and not self.uploader.allowDuplicates:
            raise DuplicateBatch(self.sitesReader.fileName)
        else:
            # get the headers
            headers = self.df.columns.values
            self.sitesReader.readBatch(headers)

            self.uploadBatchHelper()
            self.batchNumber = self.getBatchNumber()

    def duplicateRow(self):
        sqlDuplicate = "SELECT * FROM sites WHERE site_id = ?;"
        duplicateTuple = (self.sitesReader.site,)
        self.cursor.execute(sqlDuplicate, duplicateTuple)

        result = self.cursor.fetchall()
        if len(result) > 0:
            return True
        else:
            return False

    def uploadRow(self):
        sqlRow = "INSERT INTO sites (sites_batch_id, site_id, latitude, longitude," \
                 "body_of_water, area, project_id) " \
                 "VALUES (?,?,?,?,?,?,?);"

        rowTuple = (self.batchNumber, self.sitesReader.site, self.sitesReader.y, self.sitesReader.x,
                    self.sitesReader.bodyOfWater, self.sitesReader.area, self.uploader.getProjectId())

        self.cursor.execute(sqlRow, rowTuple)

    def updateRow(self):
        sqlRow = "UPDATE sites SET sites_batch_id = ?, latitude = ?, longitude = ?," \
                 "body_of_water = ?, area = ?, project_id = ? " \
                 "WHERE site_id = ?;"

        rowTuple = (self.batchNumber,self.sitesReader.y, self.sitesReader.x, self.sitesReader.bodyOfWater,
                    self.sitesReader.area, self.uploader.getProjectId(),  self.sitesReader.site)

        self.cursor.execute(sqlRow, rowTuple)

    def uploadReads(self):
        self.problemRows = []
        self.repeatRows = []

        for index, row in self.df.iterrows():
            result = self.sitesReader.readRow(list(row))
            if result == self.sitesReader.noErrors:
                if self.duplicateRow() and not self.uploader.allowDuplicates:
                    self.repeatRows.append(index)
                elif self.duplicateRow() and self.uploader.allowDuplicates:
                    self.updateRow()
                else:
                    try:
                        self.uploadRow()
                    except:
                        self.problemRows.append(index)
            else:
                self.problemRows.append(index)


        message = ""
        if len(self.problemRows) > 0:
            message = message + "ERROR: the following rows were likely missing critical values and were therefore not " \
                      "uploaded to the database: " + str(self.problemRows) + "\n"
        if len(self.repeatRows) > 0:
            message = message + "ERROR: the following rows were duplicates of sort-chems already present in the database, " \
                      "and were therefore not uploaded: " + str(self.repeatRows) + ". If you would like to upload" \
                      " these rows anyway (e.g. in the case of data correction), please select \'allow duplicates\'" \
                      " above and resubmit.\n"

        if message != "":
            raise Warnings(message, self.sitesReader.fileName)