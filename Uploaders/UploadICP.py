import pandas as pd
from CustomErrors import *

class UploadICP:
    def __init__(self, cursor, uploader, icpReader):
        self.cursor = cursor
        self.uploader = uploader
        self.icpReader = icpReader
        self.df = None

    def uploadBatch(self):
        # open the file, save as a dataframe
        self.df = pd.read_excel(self.icpReader.getFilePath())

        # go through column 0 until you find the date, and the water ID. Store those.
        cutoffIndex = None
        self.bastchDate = None
        self.batchId = None

        for i in range(20):
            value = str(self.df.iloc[i,0]).lower()
            if "date" in value:
                self.batchDate = value
            if "water id:" in value or "work" in value:
                self.batchId = value
            if ("customer" in value) and ("sample" in value) or (("date" in value) and ("time" in value) and ("and" in value)):
                cutoffIndex = i
        if cutoffIndex == None:
            raise ICPFormatChanged(self.icpReader.getFileName())

        # trim the unneccesary values off the top
        headers = list(self.df.iloc[cutoffIndex,:])
        oldHeaders = self.df.columns.values
        oldToNewDict = dict(zip(oldHeaders,headers))

        self.df = self.df.iloc[(cutoffIndex + 1):,:]
        self.df = self.df.rename(index=str, columns=oldToNewDict)

        # read the batch (and other 'under the hood' information)
        self.icpReader.readBatch(self.df.columns.values)


        # check for duplicates
        sqlBatch = "SELECT icp_batch_id FROM icp_batches WHERE date_run = ? AND " \
                   "project_id = ? AND operator = ? AND file_name = ?;"
        batchTuple = (self.icpReader.runDate, self.icpReader.projectId,
                       self.icpReader.operator, self.icpReader.fileName)

        self.cursor.execute(sqlBatch, batchTuple)
        batches = self.cursor.fetchall()

        if not self.uploader.allowDuplicates:
            if len(batches) > 0:
                raise DuplicateNotAllowed(self.icpReader.fileName)


        # upload the batch
        sqlBatch = "INSERT INTO icp_batches (date_run, project_id, operator, file_name, file_path) VALUES (?,?,?,?,?)"
        batchTuple = (
            self.icpReader.runDate,
            self.icpReader.projectId,
            self.icpReader.operator,
            self.icpReader.fileName,
            self.icpReader.filePath
        )

        self.cursor.execute(sqlBatch, batchTuple)

        # retreive the batch id
        sqlBatch = "SELECT icp_batch_id FROM icp_batches WHERE date_run = ? AND " \
                   "project_id = ? AND operator = ? AND file_name = ?;"
        batchTuple = (self.icpReader.runDate, self.icpReader.projectId,
                       self.icpReader.operator, self.icpReader.fileName)

        self.cursor.execute(sqlBatch, batchTuple)
        currentBatches = self.cursor.fetchall()
        self.currentBatch = currentBatches[-1][0]


    def uploadReads(self):
        problemRows = []
        for index, row in self.df.iterrows():
            self.icpReader.readRow(row)

            sqlRow = "INSERT INTO icp_reads (icp_batch_id, sort_chem, aluminum, arsenic, barium, boron, calcium, cadmium, cobalt, chromium, copper, " \
                     "iron, potassium, magnesium, manganese, molybdenum, sodium, nickel, phosphorus, lead, sulfur, " \
                     "selenium, silicon, strontium, titanium, vanadium, zinc) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?" \
                     ",?,?,?,?,?,?,?,?,?,?,?)"
            rowTuple = (self.currentBatch, self.icpReader.sortChem,
                        self.icpReader.aluminum, self.icpReader.arsenic, self.icpReader.barium, self.icpReader.boron, self.icpReader.calcium,
                        self.icpReader.cadmium, self.icpReader.cobalt, self.icpReader.chromium, self.icpReader.copper,
                        self.icpReader.iron, self.icpReader.potassium, self.icpReader.magnesium, self.icpReader.manganese,
                        self.icpReader.molybdenum, self.icpReader.sodium, self.icpReader.nickel, self.icpReader.phosphorus,
                        self.icpReader.lead, self.icpReader.sulfur, self.icpReader.selenium, self.icpReader.silicon,
                        self.icpReader.strontium, self.icpReader.titanium, self.icpReader.vanadium, self.icpReader.zinc)

            try:
                self.cursor.execute(sqlRow, rowTuple)
                # insert into the sort_chem_to_batch table
                sqlSort = "INSERT INTO sort_chems_to_icp_batches (sort_chem, icp_batch_id) VALUES(?,?)"
                sortTuple = (self.icpReader.sortChem, self.currentBatch)
                self.cursor.execute(sqlSort, sortTuple)

                # add to the sort-chem table **************************8
                # see if the sort chem is already in the sort-chems
                sqlCheck = "SELECT * FROM sort_chems WHERE sort_chem = ?;"
                checkTuple = (self.icpReader.sortChem,)

                self.cursor.execute(sqlCheck, checkTuple)
                sortChems = self.cursor.fetchall()

                # if not, upload it
                if len(sortChems) == 0:
                    sqlSort = "INSERT INTO sort_chems (sort_chem) VALUES (?);"
                    sortTuple = (self.icpReader.sortChem,)
                    self.cursor.execute(sqlSort, sortTuple)

            except:
                problemRows.append(index)





        if len(problemRows) > 0:
            message = "ERROR: The following rows were not uploaeded because they were missing critical values: " + str(problemRows)
            raise Warnings(message, self.icpReader.fileName)
