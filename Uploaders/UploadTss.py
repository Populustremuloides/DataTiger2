from datetime import datetime
import pandas as pd
from CustomErrors import *

# FIXME still need to edit the database to add COLUMN for tss etc
class UploadTss:
    def __init__(self, cursor, uploader, tssReader):
        self.cursor = cursor
        self.uploader = uploader
        self.tssReader = tssReader
        self.batchNumber = None

        self.noErrors = 0
        self.error = 1

        # open the file as a pandas dataframe object
        try:
            self.df = pd.read_excel(self.tssReader.filePath)
        except:
            self.df = pd.read_csv(self.tssReader.filePath)

    def uploadBatch(self):
        # register the headers
        headers = list(self.df.columns.values)
        result = self.tssReader.readBatch(headers)

        if result == self.tssReader.noErrors:

            # upload the file as a batch
            self.datetimeUploaded = str(datetime.now())
            sqlBatch = "INSERT INTO tss_batches (file_name, datetime_uploaded) VALUES (?,?);"
            batchTuple = (self.tssReader.filePath, self.datetimeUploaded)
            self.cursor.execute(sqlBatch, batchTuple)

            # get the batch id
            sqlBatch = "SELECT tss_batch_id FROM tss_batches WHERE file_name = ? AND datetime_uploaded = ?;"
            batchTuple = (self.tssReader.filePath, self.datetimeUploaded)
            self.cursor.execute(sqlBatch, batchTuple)
            self.batchNumber = self.cursor.fetchall()[0][0]

        else:
            raise TssMissingColumns(self.tssReader.fileName)

    def rowIsDuplicate(self):
        sqlDuplicate = "SELECT * FROM tss_reads WHERE sort_chem = ?;"
        duplicateTuple = (self.tssReader.sortChem,)

        self.cursor.execute(sqlDuplicate, duplicateTuple)
        result = self.cursor.fetchall()

        if len(result) > 0:
            return True
        else:
            return False

    def uploadRow(self):
        sqlRow = "INSERT INTO tss_reads (tss_batch_id," \
                 "sort_chem, site_id, user_name, date_collected," \
                 "sample_volume, initial_filter_weight, final_filter_weight, tss," \
                 "notes, drying_notes) " \
                 "VALUES (?,?,?,?,?,?,?,?,?,?,?);"
        rowTuple = (self.batchNumber, self.tssReader.sortChem,
                    self.tssReader.siteId, self.tssReader.userName,
                    self.tssReader.dateCollected, self.tssReader.sampleVolume,
                    self.tssReader.initialFilterWeight, self.tssReader.finalFilterWeight,
                    self.tssReader.tss, self.tssReader.notes,
                    self.tssReader.dryingNotes)
        """
        sqlSort = "INSERT INTO sort_chems_to_tss_batches (sort_chem, tss_batch_id) VALUES (?,?);"
        sortTuple = (self.tssReader.sortChem, self.batchNumber)

        sqlCheck = "SELECT * FROM sort_chems WHERE sort_chem = ?;"
        checkTuple = (self.tssReader.sortChem,)

        sqlSort1 = "INSERT INTO sort_chems (sort_chem) VALUES (?);"
        sort1Tuple = (self.tssReader.sortChem,)
        """
        try:
            self.cursor.execute(sqlRow, rowTuple)
            """
            self.cursor.execute(sqlSort, sortTuple)

            # add to the sort-chem table **************************8
            # see if the sort chem is already in the sort-chems
            self.cursor.execute(sqlCheck, checkTuple)
            sortChems = self.cursor.fetchall()

            # if not, upload it
            if len(sortChems) == 0:
                self.cursor.execute(sqlSort1, sort1Tuple)
            """
            return self.noErrors

        except:
            return self.error

    def uploadReads(self):
        self.problemRows = []
        self.duplicateRows = []
        for index, row in self.df.iterrows():
            result = self.tssReader.readRow(list(row))
            if result == self.tssReader.noErrors:

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
            message = message + "ERROR: The following rows were not " \
                                "uploaded because they were missing critical values: " + str(self.problemRows) + "\n"
        if len(self.duplicateRows) > 0:
            message = message + "ERROR: the following rows were duplicates of sort-chems already present in the database, " \
            "and were therefore not uploaded: " + str(self.duplicateRows) + ". If you would like to upload" \
            " these rows anyway (e.g. in the case of data correction), please select \'allow duplicates\'" \
            " above and resubmit.\n"
        if message != "":
            raise Warnings(message, self.tssReader.fileName)
