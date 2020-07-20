import pandas as pd
from CustomErrors import *

class UploadNo3:
    def __init__(self, cursor, uploader, no3Reader):
        self.cursor = cursor
        self.uploader = uploader
        self.no3Reader = no3Reader
        self.batchNumber = None

        self.noErrors = 0
        self.error = 1

        # open the file as a pandas dataframe object
        self.df = pd.read_excel(self.no3Reader.filePath)


    def batchAlreadyPresent(self):
        sqlBatch = "SELECT * FROM no3_batches WHERE file_name = ?;"
        batchTuple = (self.no3Reader.fileName,)

        self.cursor.execute(sqlBatch, batchTuple)
        result = self.cursor.fetchall()

        if len(result) > 0:
            return True
        else:
            return False

    def uploadBatchHelper(self):
        sqlBatch = "INSERT INTO no3_batches (file_name, file_path, " \
                   "project_id, datetime_uploaded) VALUES (?,?,?,?);"
        batchTuple = (self.no3Reader.fileName, self.no3Reader.filePath,
                      self.uploader.getProjectId(), self.no3Reader.datetimeUploaded)

        self.cursor.execute(sqlBatch, batchTuple)

    def getBatchNumber(self):
        sqlBatch = "SELECT no3_batch_id FROM no3_batches WHERE datetime_uploaded = ?;"
        batchTuple = (self.no3Reader.datetimeUploaded,)

        self.cursor.execute(sqlBatch, batchTuple)
        batches = self.cursor.fetchall()

        return batches[0][0]

    def uploadBatch(self):

        # register the header indices
        headers = list(self.df.columns.values)

        if self.batchAlreadyPresent() and not self.uploader.allowDuplicates:
            raise DuplicateBatch(self.no3Reader.fileName)
        else:
            result = self.no3Reader.readBatch(headers)
            if result == self.no3Reader.missingHeaders:
                raise No3NotFormattedCorrectly(self.no3Reader.fileName)
            else:
                self.uploadBatchHelper()
                self.batchNumber = self.getBatchNumber()

    def duplicateRow(self):
        sqlDuplicate = "SELECT * FROM no3_reads WHERE sort_chem = ?;"
        duplicateTuple = (self.no3Reader.sortChem,)
        self.cursor.execute(sqlDuplicate, duplicateTuple)

        result = self.cursor.fetchall()
        if len(result) > 0:
            return True
        else:
            return False

    def uploadRow(self):
        sqlRow = "INSERT INTO no3_reads (no3_batch_id," \
                 "sort_chem, d15n_air, d18_ovsmow, comments) VALUES (?,?,?,?,?);"
        rowTuple = (self.batchNumber, self.no3Reader.sortChem,
                    self.no3Reader.delta15, self.no3Reader.delta18, self.no3Reader.notes)
        self.cursor.execute(sqlRow, rowTuple)

    def uploadSortChemToBatch(self):
        sqlSort = "INSERT INTO sort_chems_to_no3_batches (sort_chem, no3_batch_id)" \
                  " VALUES (?,?);"
        sortTuple = (self.no3Reader.sortChem, self.batchNumber)
        self.cursor.execute(sqlSort, sortTuple)


    def uploadSortChem(self):
        sqlUnique = "SELECT * FROM sort_chems WHERE sort_chem = ?;"
        uniqueTuple = (self.no3Reader.sortChem,)
        self.cursor.execute(sqlUnique, uniqueTuple)
        result = self.cursor.fetchall()

        if len(result) == 0:

            sqlSort = "INSERT INTO sort_chems (sort_chem) VALUES (?);"
            sortTuple = (self.no3Reader.sortChem,)
            self.cursor.execute(sqlSort, sortTuple)

    def uploadReads(self):

        problemRows = []
        duplicateRows = []
        for index, row in self.df.iterrows():
            result = self.no3Reader.readRow(row)

            if result == self.no3Reader.missingValues:
                problemRows.append(index)
            else:
                if self.duplicateRow() and not self.uploader.allowDuplicates:
                    duplicateRows.append(index)
                else:
                    self.uploadRow()
                    self.uploadSortChemToBatch()
                    self.uploadSortChem()
        message = ""
        if len(problemRows) > 0:
            message = message + "WARNING: the following rows were likely missing critical values and were therefore not " \
                      "uploaded to the database: " + str(problemRows) + "\n"
        if len(duplicateRows) > 0:
            message = message + "WARNING: the following rows were duplicates of sort-chems already present in the database, " \
                      "and were therefore not uploaded: " + str(duplicateRows) + ". If you would like to upload" \
                      " these rows anyway (e.g. in the case of data correction), please select \'allow duplicates\'" \
                      " above and resubmit.\n"

        if message != "":
            raise Warnings(message, self.no3Reader.fileName)
