import pandas as pd
from CustomErrors import *

class UploadScanMaster:
    def __init__(self, cursor, uploader, scanMasterReader):
        self.cursor = cursor
        self.uploader = uploader
        self.scanMasterReader = scanMasterReader
        self.df = None
        self.semicolon = False
        self.tab = False

        self.currentBatch = None

        # separate the different excel sheets
        if self.scanMasterReader.filePath.endswith(".csv"):
            pass
        else:
            xls = pd.ExcelFile(self.scanMasterReader.filePath)
            self.sheets = xls.sheet_names
            self.data = None
        self.errorOccurred = False


    def uploadBatch(self):
        # get the data
        if self.scanMasterReader.filePath.endswith(".csv"):
            self.data = pd.read_csv(self.scanMasterReader.filePath)
        else:
            self.data = pd.read_excel(self.scanMasterReader.filePath, self.sheets[0])

        # get the info about the batch
        columns = list(self.data.columns.values)
        self.scanMasterReader.readBatch(columns)

        # upload the file
        sqlBatch = "INSERT INTO scan_master_batches (file_name, file_path, upload_datetime) VALUES (?,?,?);"
        batchTuple = (self.scanMasterReader.fileName, self.scanMasterReader.filePath, self.scanMasterReader.datetime)
        self.cursor.execute(sqlBatch, batchTuple)

        # retain the batch_id
        sqlBatch = "SELECT scan_master_batch_id FROM scan_master_batches WHERE file_name = ? AND file_path = ? AND upload_datetime = ?;"
        batchTuple = (self.scanMasterReader.fileName, self.scanMasterReader.filePath, self.scanMasterReader.datetime)
        self.cursor.execute(sqlBatch, batchTuple)

        # get the batch id
        currentBatches = self.cursor.fetchall()
        self.currentBatch = currentBatches[-1][0]

    def sortChemOnDatabase(self):
        sqlCheck = "SELECT * FROM sort_chems WHERE sort_chem = ?;"
        checkTuple = (self.scanMasterReader.sortChem,)

        self.cursor.execute(sqlCheck, checkTuple)
        sortChems = self.cursor.fetchall()

        if len(sortChems) == 0:
            return False
        else:
            return True

    def uploadSortChem(self):
        sqlSort = "INSERT INTO sort_chems (sort_chem) VALUES (?);"
        sortTuple = (self.scanMasterReader.sortChem,)
        self.cursor.execute(sqlSort, sortTuple)

    def getDuplicateBatch(self):
        sqlBatch = "SELECT datetime_run FROM sort_chems_to_datetime_run WHERE datetime_run = ? AND sort_chem = ?;"
        batchTuple = (str(self.scanMasterReader.timestamp), self.scanMasterReader.sortChem)

        self.cursor.execute(sqlBatch, batchTuple)
        batches = self.cursor.fetchall()
        return batches

    def uploadRow(self):
        # check for duplicates first

        # check for duplicate datetime values

        batches = self.getDuplicateBatch()

        if (len(batches) > 0) and (not self.uploader.allowDuplicates):
            print("duplicate")
            print(self.scanMasterReader.sortChem)
            return self.scanMasterReader.error

        elif (len(batches) > 0) and self.uploader.allowDuplicates:

            sqlUpdate = "UPDATE sort_chems_to_datetime_run SET datetime_run = ?, scan_master_batch_id = ? WHERE sort_chem = ?;"
            updateTuple = (str(self.scanMasterReader.timestamp), self.currentBatch, self.scanMasterReader.sortChem)
            # try:
            self.cursor.execute(sqlUpdate, updateTuple)
            # except:
            #     print("update error")
            #     print(self.scanMasterReader.sortChem)
            #     return self.scanMasterReader.error


            if not self.sortChemOnDatabase():
                self.uploadSortChem()

            return self.scanMasterReader.noError

        else:
            # upload the row
            sqlRow = "INSERT INTO sort_chems_to_datetime_run (sort_chem, datetime_run, scan_master_batch_id) VALUES (?,?,?);"
            rowTuple = (self.scanMasterReader.sortChem, str(self.scanMasterReader.timestamp), self.currentBatch)
            try:
                self.cursor.execute(sqlRow, rowTuple)
            except:
                print("insert error")
                return self.scanMasterReader.error

            if not self.sortChemOnDatabase():
                self.uploadSortChem()

            return self.scanMasterReader.noError

    def uploadReads(self):
        self.errorOccurred = False
        problematicRows = []
        repeatedRows = []

        # go through every row of the data
        for index, row in self.data.iterrows():

            result = self.scanMasterReader.readDataRow(row)
            # make sure there were no errors when parsing the data
            if result == self.scanMasterReader.error:
                problematicRows.append(index + 2)
                self.errorOccurred = True

            elif result == self.scanMasterReader.noError:
                # attempt to upload the data
                uploadResult = self.uploadRow()

                # make sure there were no errors when uploading the data
                if uploadResult == self.scanMasterReader.error:
                    self.errorOccurred = True
                    repeatedRows.append(index + 2)

        message = ""
        if len(problematicRows) > 0:
            message = message + "ERROR: The following rows were missing critical information and were not " \
                                "uploaded to the database: " + str(problematicRows) + "\n"
        if len(repeatedRows) > 0:
            message = message + "ERROR: The following rows included date-times that were already present on the" \
                    " database, and were therefore not added: " + str(repeatedRows) + ". If you would like to override those rows, please " \
                    "select \'allow duplicates\' above and re-submit the file.\n"
        if message != "":
            raise Warnings(message, self.scanMasterReader.fileName)




