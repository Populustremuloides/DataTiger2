import pandas as pd
from CustomErrors import *

class UploadNewSrp:
    def __init__(self, cursor, uploader, newSrpReader):
        self.cursor = cursor
        self.uploader = uploader
        self.newSrpReader = newSrpReader
        self.batchNumber = None

        self.noErrors = 0
        self.error = 1

    def duplicateBatch(self):
        # see if the file name and project have already been uploaded
        sqlBatch = "SELECT * FROM new_srp_batches WHERE file_name = ?;"
        batchTuple = (self.newSrpReader.fileName,)

        self.cursor.execute(sqlBatch, batchTuple)
        result = self.cursor.fetchall()

        if len(result) > 0:
            return True
        else:
            return False

    def getHeaders(self):
        first_col = self.df.iloc[:,0]
        first_col = first_col.values.tolist()

        for i in range(20):
            value = first_col[i].lower() if type(first_col[i])==str else ""
            if "customer" in value and "sample" in value and "id" in value:
                headers = self.df.iloc[i].values.tolist()
                preheaders = self.df.iloc[i-1].values.tolist()
                self.df = self.df.iloc[(i + 1):, :]
                self.newSrpReader.readBatch(headers, preheaders)
        return

    def uploadBatch(self):
        # check for duplicates
        if self.duplicateBatch() and not self.uploader.allowDuplicates:
            raise DuplicateBatch(self.newSrpReader.fileName)
        else:
            # get the headers
            exl = pd.ExcelFile(self.newSrpReader.filePath)
            sheetNames = exl.sheet_names
            self.df = pd.read_excel(exl, sheetNames[0])
            self.getHeaders()

            # upload the batch
            sqlBatch = "INSERT INTO new_srp_batches " \
                       "(project_id, file_name, file_path, datetime_uploaded) VALUES (?,?,?,?);"
            batchTuple = (self.newSrpReader.projectId, self.newSrpReader.fileName,
                          self.newSrpReader.filePath, self.newSrpReader.datetimeUploaded)

            self.cursor.execute(sqlBatch, batchTuple)

            # retrieve the batch id
            sqlBatch = "SELECT new_srp_batch_id FROM new_srp_batches WHERE datetime_uploaded = ? AND " \
                       "project_id = ? AND file_name = ?;"
            batchTuple = (self.newSrpReader.datetimeUploaded, self.newSrpReader.projectId,
                          self.newSrpReader.fileName)

            self.cursor.execute(sqlBatch, batchTuple)
            currentBatches = self.cursor.fetchall()
            self.batchNumber = currentBatches[-1][0]

    def uploadReads(self):
        self.problemRows = []

        for index, row in self.df.iterrows():
            self.newSrpReader.readRow(row)

            sqlRow = "INSERT INTO new_srp_reads (new_srp_batch_id, sort_chem, no3, nh4, srp) VALUES (?,?,?,?,?)"
            rowTuple = (self.batchNumber, self.newSrpReader.sortchem, self.newSrpReader.no3, self.newSrpReader.nh4, self.newSrpReader.srp)

            # try:
            self.cursor.execute(sqlRow, rowTuple)
            # insert into the sort_chem_to_batch table
            sqlSort = "INSERT INTO sort_chems_to_new_srp_batches (sort_chem, new_srp_batch_id) VALUES(?,?)"
            sortTuple = (self.newSrpReader.sortchem, self.batchNumber)
            self.cursor.execute(sqlSort, sortTuple)

            # add to the sort-chem table **************************8
            # see if the sort chem is already in the sort-chems
            sqlCheck = "SELECT * FROM sort_chems WHERE sort_chem = ?;"
            checkTuple = (self.newSrpReader.sortchem,)

            self.cursor.execute(sqlCheck, checkTuple)
            sortchems = self.cursor.fetchall()

            # if not, upload it
            if len(sortchems) == 0:
                sqlSort = "INSERT INTO sort_chems (sort_chem) VALUES (?);"
                sortTuple = (self.newSrpReader.sortchem,)
                self.cursor.execute(sqlSort, sortTuple)
            else:
                self.problemRows.append(index)

        message = ""
        if len(self.problemRows) > 0:
            message = message + "ERROR: the following rows' sortchems were already present in the database and thus were not uploaded to sort_chems (the data was still saved) " + str(self.problemRows) + "\n"
        if message != "":
            raise Warnings(message, self.newSrpReader.fileName)
