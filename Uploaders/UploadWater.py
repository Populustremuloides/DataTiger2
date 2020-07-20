import pandas as pd
from CustomErrors import *

class UploadWater:
    def __init__(self, cursor, uploader, waterReader):
        self.cursor = cursor
        self.uploader = uploader
        self.waterReader = waterReader
        self.batchNumber = None

        self.noErrors = 0
        self.error = 1

        # open the file as a pandas dataframe object
        self.df = pd.read_excel(self.waterReader.filePath)

    def duplicateBatch(self):
        # see if the file name and project have already been uploaded
        sqlBatch = "SELECT * FROM water_isotopes_batches WHERE file_name = ?;"
        batchTuple = (self.waterReader.fileName,)

        self.cursor.execute(sqlBatch, batchTuple)
        result = self.cursor.fetchall()

        if len(result) > 0:
            return True
        else:
            return False

    def uploadBatchHelper(self):
        sqlBatch = "INSERT INTO water_isotopes_batches " \
                   "(project_id, file_name, file_path, datetime_uploaded) VALUES (?,?,?,?);"
        batchTuple = (self.uploader.getProjectId(), self.waterReader.fileName,
                      self.waterReader.filePath, self.waterReader.datetimeUploaded)

        self.cursor.execute(sqlBatch, batchTuple)

    def getBatchNumber(self):
        sqlBatch = "SELECT water_isotopes_batch_id FROM water_isotopes_batches WHERE " \
                   "datetime_uploaded = ?;"
        batchTuple = (self.waterReader.datetimeUploaded,)

        self.cursor.execute(sqlBatch, batchTuple)
        result = self.cursor.fetchall()[0][0]
        return result

    def uploadBatch(self):
        # check for duplicates
        if self.duplicateBatch() and not self.uploader.allowDuplicates:
            raise DuplicateBatch(self.waterReader.fileName)
        else:
            # get the headers
            headers = self.df.columns.values
            self.waterReader.readBatch(headers)

            self.uploadBatchHelper()
            self.batchNumber = self.getBatchNumber()

    def duplicateRow(self):
        sqlDuplicate = "SELECT * FROM water_isotopes_reads WHERE sort_chem = ?;"
        duplicateTuple = (self.waterReader.sortChem,)
        self.cursor.execute(sqlDuplicate, duplicateTuple)

        result = self.cursor.fetchall()
        if len(result) > 0:
            return True
        else:
            return False

    def uploadRow(self):
        sqlRow = "INSERT INTO water_isotopes_reads (water_isotopes_batch_id," \
                 "sort_chem, datetime_run, d18O, d18O_error, dd, dd_error) VALUES (?,?,?,?,?,?,?);"

        rowTuple = (self.batchNumber, self.waterReader.sortChem, self.waterReader.analysisDate, self.waterReader.d18O,
                    self.waterReader.d18OError, self.waterReader.dD, self.waterReader.dDError)

        self.cursor.execute(sqlRow, rowTuple)

    def uploadSortChemToBatch(self):
        sqlSort = "INSERT INTO sort_chems_to_water_isotopes_batches (sort_chem, water_isotopes_batch_id)" \
                  " VALUES (?,?);"
        sortTuple = (self.waterReader.sortChem, self.batchNumber)
        self.cursor.execute(sqlSort, sortTuple)


    def uploadSortChem(self):
        sqlUnique = "SELECT * FROM sort_chems WHERE sort_chem = ?;"
        uniqueTuple = (self.waterReader.sortChem,)
        self.cursor.execute(sqlUnique, uniqueTuple)
        result = self.cursor.fetchall()

        if len(result) == 0:

            sqlSort = "INSERT INTO sort_chems (sort_chem) VALUES (?);"
            sortTuple = (self.waterReader.sortChem,)
            self.cursor.execute(sqlSort, sortTuple)

    def uploadReads(self):
        self.problemRows = []
        self.repeatRows = []

        for index, row in self.df.iterrows():
            result = self.waterReader.readRow(list(row))
            if result == self.waterReader.noErrors:
                if self.duplicateRow() and not self.uploader.allowDuplicates:
                    self.repeatRows.append(index)
                else:
                    try:
                        self.uploadRow()
                        self.uploadSortChemToBatch()
                        self.uploadSortChem()
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
            raise Warnings(message, self.waterReader.fileName)