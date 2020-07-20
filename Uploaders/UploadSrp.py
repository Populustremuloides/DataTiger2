import pandas as pd
from CustomErrors import *

class UploadSrp:
    def __init__(self, cursor, uploader, srpReader):
        self.cursor = cursor
        self.uploader = uploader
        self.srpReader = srpReader
        self.batchNumber = None

        self.noErrors = 0
        self.error = 1

        # open the file as a pandas dataframe object
        self.df = pd.read_excel(self.srpReader.filePath)

    def duplicateBatch(self):
        # see if the file name and project have already been uploaded
        sqlBatch = "SELECT * FROM srp_batches WHERE file_name = ?;"
        batchTuple = (self.srpReader.fileName,)

        self.cursor.execute(sqlBatch, batchTuple)
        result = self.cursor.fetchall()

        if len(result) > 0:
            return True
        else:
            return False

    def uploadBatchHelper(self):
        sqlBatch = "INSERT INTO srp_batches " \
                   "(project_id, file_name, file_path, datetime_uploaded) VALUES (?,?,?,?);"
        batchTuple = (self.uploader.getProjectId(), self.srpReader.fileName,
                      self.srpReader.filePath, self.srpReader.datetimeUploaded)

        self.cursor.execute(sqlBatch, batchTuple)

    def getBatchNumber(self):
        sqlBatch = "SELECT srp_batch_id FROM srp_batches WHERE " \
                   "datetime_uploaded = ?;"
        batchTuple = (self.srpReader.datetimeUploaded,)

        self.cursor.execute(sqlBatch, batchTuple)
        result = self.cursor.fetchall()[0][0]
        return result

    def uploadBatch(self):
        # check for duplicates
        if self.duplicateBatch() and not self.uploader.allowDuplicates:
            raise DuplicateBatch(self.srpReader.fileName)
        else:
            # get the headers
            headers = self.df.columns.values
            self.srpReader.readBatch(headers)

            self.uploadBatchHelper()
            self.batchNumber = self.getBatchNumber()

    def duplicateRow(self):
        sqlDuplicate = "SELECT * FROM srp_reads WHERE sort_chem = ?;"
        duplicateTuple = (self.srpReader.sortChem,)
        self.cursor.execute(sqlDuplicate, duplicateTuple)

        result = self.cursor.fetchall()
        if len(result) > 0:
            return True
        else:
            return False

    def uploadRow(self):
        sqlRow = "INSERT INTO srp_reads (srp_batch_id," \
                 "sort_chem, analysis_date, absorbance, concentration," \
                 "std_1_concentration, std_2_concentration, std_3_concentration," \
                 "std_4_concentration, std_5_concentration, std_6_concentration," \
                 "std_1_absorbance, std_2_absorbance, std_3_absorbance," \
                 "std_4_absorbance, std_5_absorbance, std_6_absorbance) " \
                 "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"

        rowTuple = (self.batchNumber, self.srpReader.sortChem, str(self.srpReader.analysisDate),
                    self.srpReader.absorbance, self.srpReader.concentration,
                    self.srpReader.std1Concentration, self.srpReader.std2Concentration,
                    self.srpReader.std3Concentration, self.srpReader.std4Concentration,
                    self.srpReader.std5Concentration, self.srpReader.std6Concentration,
                    self.srpReader.std1Absorbance, self.srpReader.std2Absorbance,
                    self.srpReader.std3Absorbance, self.srpReader.std4Absorbance,
                    self.srpReader.std5Absorbance, self.srpReader.std6Absorbance)

        self.cursor.execute(sqlRow, rowTuple)

    def uploadSortChemToBatch(self):
        sqlSort = "INSERT INTO sort_chems_to_srp_batches (sort_chem, srp_batch_id)" \
                  " VALUES (?,?);"
        sortTuple = (self.srpReader.sortChem, self.batchNumber)
        self.cursor.execute(sqlSort, sortTuple)


    def uploadSortChem(self):
        sqlUnique = "SELECT * FROM sort_chems WHERE sort_chem = ?;"
        uniqueTuple = (self.srpReader.sortChem,)
        self.cursor.execute(sqlUnique, uniqueTuple)
        result = self.cursor.fetchall()

        if len(result) == 0:

            sqlSort = "INSERT INTO sort_chems (sort_chem) VALUES (?);"
            sortTuple = (self.srpReader.sortChem,)
            self.cursor.execute(sqlSort, sortTuple)

    def uploadReads(self):
        self.problemRows = []
        self.repeatRows = []

        for index, row in self.df.iterrows():
            result = self.srpReader.readRow(list(row))
            if result == self.srpReader.noErrors:
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
            raise Warnings(message, self.srpReader.fileName)