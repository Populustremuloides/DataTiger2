import pandas as pd
from CustomErrors import *

class UploadLachat:
    def __init__(self, cursor, uploader, lachatReader):
        self.cursor = cursor
        self.uploader = uploader
        self.lachatReader = lachatReader
        self.batchNumber = None

        self.noErrors = 0
        self.error = 1

        # open the file as a pandas dataframe object
        self.df = pd.read_excel(self.lachatReader.filePath)

        # trim the dataframe
        self.trimDataframe()

    def trimDataframe(self):
        headers = None
        startOfData = None

        i = 0
        while i < self.df.shape[0]: # read through the first column
            value = str(self.df.iloc[i,0]).lower()
            if "sample id" in value:
                headers = list(self.df.iloc[i,])
                startOfData = i + 1
                break

            i = i + 1

        if headers != None and startOfData != None:
            # trim the unnecessary rows
            self.df = self.df.iloc[startOfData:,]

            # rename the columns
            oldHeaders = self.df.columns.values
            oldToNew = dict(zip(oldHeaders, headers))
            self.df = self.df.rename(index=str, columns=oldToNew)

        else:
            raise LachatNotFormattedCorrectly(self.lachatReader.fileName)

    def batchAlreadyPresent(self):
        sqlBatch = "SELECT * FROM lachat_batches WHERE file_name = ?;"
        batchTuple = (self.lachatReader.fileName,)

        self.cursor.execute(sqlBatch, batchTuple)
        result = self.cursor.fetchall()

        if len(result) > 0:
            return True
        else:
            return False

    def uploadBatchHelper(self):
        sqlBatch = "INSERT INTO lachat_batches (file_name, file_path, " \
                   "project_id, datetime_uploaded) VALUES (?,?,?,?);"
        batchTuple = (self.lachatReader.fileName, self.lachatReader.filePath,
                      self.uploader.getProjectId(), self.lachatReader.datetimeUploaded)

        self.cursor.execute(sqlBatch, batchTuple)

    def getBatchNumber(self):
        sqlBatch = "SELECT lachat_batch_id FROM lachat_batches WHERE datetime_uploaded = ?;"
        batchTuple = (self.lachatReader.datetimeUploaded,)

        self.cursor.execute(sqlBatch, batchTuple)
        batches = self.cursor.fetchall()

        return batches[0][0]

    def uploadBatch(self):

        # register the header indices
        headers = list(self.df.columns.values)

        if self.batchAlreadyPresent() and not self.uploader.allowDuplicates:
            raise DuplicateBatch(self.lachatReader.fileName)
        else:
            result = self.lachatReader.readBatch(headers)
            if result == self.lachatReader.missingHeaders:
                raise LachatNotFormattedCorrectly(self.lachatReader.fileName)
            else:
                self.uploadBatchHelper()
                self.batchNumber = self.getBatchNumber()

    def duplicateRow(self):
        sqlDuplicate = "SELECT * FROM lachat_reads WHERE sort_chem = ?;"
        duplicateTuple = (self.lachatReader.sortChem,)
        self.cursor.execute(sqlDuplicate, duplicateTuple)

        result = self.cursor.fetchall()
        if len(result) > 0:
            return True
        else:
            return False

    def uploadRow(self):
        sqlRow = "INSERT INTO lachat_reads (lachat_batch_id," \
                 "sort_chem, no3_ppm, no4_ppm) VALUES (?,?,?,?);"
        rowTuple = (self.batchNumber, self.lachatReader.sortChem,
                    self.lachatReader.no3, self.lachatReader.no4)
        self.cursor.execute(sqlRow, rowTuple)

    def uploadSortChemToBatch(self):
        sqlSort = "INSERT INTO sort_chems_to_lachat_batches (sort_chem, lachat_batch_id)" \
                  " VALUES (?,?);"
        sortTuple = (self.lachatReader.sortChem, self.batchNumber)
        self.cursor.execute(sqlSort, sortTuple)


    def uploadSortChem(self):
        sqlUnique = "SELECT * FROM sort_chems WHERE sort_chem = ?;"
        uniqueTuple = (self.lachatReader.sortChem,)
        self.cursor.execute(sqlUnique, uniqueTuple)
        result = self.cursor.fetchall()

        if len(result) == 0:

            sqlSort = "INSERT INTO sort_chems (sort_chem) VALUES (?);"
            sortTuple = (self.lachatReader.sortChem,)
            self.cursor.execute(sqlSort, sortTuple)


    def uploadReads(self):

        problemRows = []
        duplicateRows = []
        for index, row in self.df.iterrows():
            result = self.lachatReader.readRow(row)

            if result == self.lachatReader.missingValues:
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
            message = message + "ERROR: the following rows were likely missing critical values and were therefore not " \
                      "uploaded to the database: " + str(problemRows) + "\n"
        if len(duplicateRows) > 0:
            message = message + "ERROR: the following rows were duplicates of sort-chems already present in the database, " \
                      "and were therefore not uploaded: " + str(duplicateRows) + ". If you would like to upload" \
                      " these rows anyway (e.g. in the case of data correction), please select \'allow duplicates\'" \
                      " above and resubmit.\n"

        if message != "":
            raise Warnings(message, self.lachatReader.fileName)
