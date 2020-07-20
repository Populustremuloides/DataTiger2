import pandas as pd
from CustomErrors import *

class UploadDOCIsotopes:
    def __init__(self, cursor, uploader, docReader):
        self.cursor = cursor
        self.uploader = uploader
        self.docReader = docReader
        self.batchNumber = None

        self.noErrors = 0
        self.error = 1

        # open the file as a pandas dataframe object
        self.df = pd.read_excel(self.docReader.filePath)

    def duplicateBatch(self):
        # see if the file name and project have already been uploaded
        sqlBatch = "SELECT * FROM doc_isotope_batches WHERE file_name = ?;"
        batchTuple = (self.docReader.fileName,)

        self.cursor.execute(sqlBatch, batchTuple)
        result = self.cursor.fetchall()

        if len(result) > 0:
            return True
        else:
            return False

    def uploadBatchHelper(self):
        sqlBatch = "INSERT INTO doc_isotope_batches " \
                   "(project_id, file_name, file_path, datetime_uploaded) VALUES (?,?,?,?);"
        batchTuple = (self.uploader.getProjectId(), self.docReader.fileName,
                      self.docReader.filePath, self.docReader.datetimeUploaded)

        self.cursor.execute(sqlBatch, batchTuple)

    def getBatchNumber(self):
        sqlBatch = "SELECT doc_isotope_batch_id FROM doc_isotope_batches WHERE " \
                   "datetime_uploaded = ?;"
        batchTuple = (self.docReader.datetimeUploaded,)

        self.cursor.execute(sqlBatch, batchTuple)
        result = self.cursor.fetchall()[0][0]
        return result

    def uploadBatch(self):
        # check for duplicates
        if self.duplicateBatch() and not self.uploader.allowDuplicates:
            raise DuplicateDOCBatch(self.docReader.fileName)
        else:
            # get the headers
            headers = self.df.columns.values
            self.docReader.readBatch(headers)

            self.uploadBatchHelper()
            self.batchNumber = self.getBatchNumber()

    def duplicateRow(self):
        sqlDuplicate = "SELECT * FROM doc_isotope_reads WHERE sort_chem = ?;"
        duplicateTuple = (self.docReader.sortChem,)
        self.cursor.execute(sqlDuplicate, duplicateTuple)

        result = self.cursor.fetchall()
        if len(result) > 0:
            return True
        else:
            return False

    def uploadRow(self):
        sqlRow = "INSERT INTO doc_isotope_reads (doc_isotope_batch_id," \
                 "sort_chem, internal_identifier, doc_ppm," \
                 "delta_13_c, comments) VALUES (?,?,?,?,?,?);"
        rowTuple = (self.batchNumber, self.docReader.sortChem, self.docReader.internalIdentifier,
                    self.docReader.doc, self.docReader.delta13, self.docReader.comment)
        self.cursor.execute(sqlRow, rowTuple)

    def uploadSortChemToBatch(self):
        sqlSort = "INSERT INTO sort_chems_to_doc_isotope_batches (sort_chem, doc_isotope_batch_id)" \
                  " VALUES (?,?);"
        sortTuple = (self.docReader.sortChem, self.batchNumber)
        self.cursor.execute(sqlSort, sortTuple)


    def uploadSortChem(self):
        sqlUnique = "SELECT * FROM sort_chems WHERE sort_chem = ?;"
        uniqueTuple = (self.docReader.sortChem,)
        self.cursor.execute(sqlUnique, uniqueTuple)
        result = self.cursor.fetchall()

        if len(result) == 0:

            sqlSort = "INSERT INTO sort_chems (sort_chem) VALUES (?);"
            sortTuple = (self.docReader.sortChem,)
            self.cursor.execute(sqlSort, sortTuple)

    def uploadReads(self):
        self.problemRows = []
        self.repeatRows = []

        for index, row in self.df.iterrows():
            result = self.docReader.readRow(list(row))
            if result == self.docReader.noErrors:
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
            raise Warnings(message, self.docReader.fileName)
