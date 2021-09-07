import traceback
from datetime import datetime
import pandas as pd
from CustomErrors import *

class UploadAqualog:
    def __init__(self, cursor, uploader, aqualogReader):
        self.cursor = cursor
        self.uploader = uploader
        self.aqualogReader = aqualogReader
        self.batchNumber = None

        self.noErrors = 0
        self.error = 1

        # open the file as a pandas dataframe object
        try:
            self.df = pd.read_excel(self.aqualogReader.filePath)
        except:
            self.df = pd.read_csv(self.aqualogReader.filePath)

    def uploadBatch(self):
        # register the headers
        headers = list(self.df.columns.values)
        result = self.aqualogReader.readBatch(headers)

        if result == self.aqualogReader.noErrors:

            # upload the file as a batch
            self.datetimeUploaded = str(datetime.now())
            sqlBatch = "INSERT INTO aqualog_batches (file_name, datetime_uploaded) VALUES (?,?);"
            batchTuple = (self.aqualogReader.filePath, self.datetimeUploaded)
            self.cursor.execute(sqlBatch, batchTuple)

            # get the batch id
            sqlBatch = "SELECT aqualog_batch_id FROM aqualog_batches WHERE file_name = ? AND datetime_uploaded = ?;"
            batchTuple = (self.aqualogReader.filePath, self.datetimeUploaded)
            self.cursor.execute(sqlBatch, batchTuple)
            self.batchNumber = self.cursor.fetchall()[0][0]

        else:
            raise AqualogMissingColumns(self.aqualogReader.fileName)

    def rowIsDuplicate(self):
        sqlDuplicate = "SELECT * FROM aqualog_reads WHERE sort_chem = ?;"
        duplicateTuple = (self.aqualogReader.sortChem,)

        self.cursor.execute(sqlDuplicate, duplicateTuple)
        result = self.cursor.fetchall()

        if len(result) > 0:
            return True
        else:
            return False


    def uploadRow(self):
        sqlRow = "INSERT INTO aqualog_reads (aqualog_batch_id," \
                 "sort_chem, operator, corrected_by, date_collected," \
                 "date_corrected, project_file, sample_name, corrected_name," \
                 "blank_name, dilution_factor, fi370, fi310, fi254, abs254," \
                 "slp274_295, slp350_400, sr, ese3) VALUES (?,?,?,?,?,?,?,?,?," \
                 "?,?,?,?,?,?,?,?,?,?);"
        rowTuple = (self.batchNumber, self.aqualogReader.sortChem,
                    self.aqualogReader.operator, self.aqualogReader.correctedBy,
                    self.aqualogReader.dateCollected, self.aqualogReader.dateCorrected,
                    self.aqualogReader.projectFile, self.aqualogReader.sampleName,
                    self.aqualogReader.correctedName, self.aqualogReader.blankName,
                    self.aqualogReader.dilutionFactor, self.aqualogReader.fi370,
                    self.aqualogReader.fi310, self.aqualogReader.fi254,
                    self.aqualogReader.abs254, self.aqualogReader.slp274_295,
                    self.aqualogReader.slp350_400, self.aqualogReader.sr,
                    self.aqualogReader.ese3)

        sqlSort = "INSERT INTO sort_chems_to_aqualog_batches (sort_chem, aqualog_batch_id) VALUES (?,?);"
        sortTuple = (self.aqualogReader.sortChem, self.batchNumber)

        sqlCheck = "SELECT * FROM sort_chems WHERE sort_chem = ?;"
        checkTuple = (self.aqualogReader.sortChem,)

        sqlSort1 = "INSERT INTO sort_chems (sort_chem) VALUES (?);"
        sort1Tuple = (self.aqualogReader.sortChem,)

        try:

            self.cursor.execute(sqlRow, rowTuple)
            self.cursor.execute(sqlSort, sortTuple)

            # add to the sort-chem table **************************8
            # see if the sort chem is already in the sort-chems
            self.cursor.execute(sqlCheck, checkTuple)
            sortChems = self.cursor.fetchall()

            # if not, upload it
            if len(sortChems) == 0:
                self.cursor.execute(sqlSort1, sort1Tuple)

            return self.noErrors
        except:
            print(traceback.format_exc())
            return self.error

    def uploadReads(self):
        self.problemRows = []
        self.duplicateRows = []
        for index, row in self.df.iterrows():
            result = self.aqualogReader.readRow(list(row))
            if result == self.aqualogReader.noErrors:

                # check for duplicates
                if self.rowIsDuplicate() and not self.uploader.allowDuplicates:
                    self.duplicateRows.append(index)
                else:
                    result = self.uploadRow()
                    if result == self.error:
                        self.problemRows.append(index)

            else:
                self.problemRows.append(index)


        message = ""
        if len(self.problemRows) > 0:
            message = message + "ERROR: The following " + str(len(self.problemRows)) + " rows were not " \
                                "uploaded because they were missing critical values: " + str(self.problemRows) + "\n\n"
        if len(self.duplicateRows) > 0:
            message = message + "ERROR: The following " + str(len(self.duplicateRows)) + " rows were not uploaded because they were duplicates" \
                                " of sort-chems with EEMs data already present in the " \
                                "database: " + str(self.duplicateRows) + " If you would like to " \
                                "add these rows anyway, please select \'allow duplicates\' above, and re-submit.\n\n"
        if message != "":
            raise ErrorInAqualogRows(message, self.aqualogReader.fileName)


