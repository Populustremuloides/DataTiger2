import pandas as pd
from CustomErrors import *

class UploadIC():
    def __init__(self, cursor, uploader, reader):
        self.cursor = cursor
        self.uploader = uploader
        self.icReader = reader

        self.df = None

    def getProjectId(self):
        return self.uploader.getProjectId()

    def getNewDf(self, df): # this takes a dataframe in the new format and places it in the new format
        # figure out the columns I want to keep
        amountFound = False
        includedIndices = [1]
        i = 0
        for column in df.columns.values:
            if "amount" in column.lower():
                amountFound = True
                includedIndices.append(i)
            elif amountFound:
                if "unnamed" not in column.lower():
                    amountFound = False
                else:
                    includedIndices.append(i)
            i = i + 1

        df = df.iloc[:,includedIndices]

        # figure out the rows on the top to sluff off
        df = df.iloc[2:,]
        # print(df.head())
        newHeaders = [df.columns.values[0]]
        ions = list(df.iloc[0,:])
        ions = ions[1:]
        newHeaders = newHeaders + ions
        df = df.iloc[1:,:]

        # rename everything
        newHeaders = dict(zip(df.columns.values, newHeaders))
        df = df.rename(index=str, columns=newHeaders)

        return df


    def uploadBatch(self):

        # open the file as a pandas dataframe, save the dataframe
        xls = pd.ExcelFile(self.icReader.filePath)
        sheets = xls.sheet_names

        if len(sheets) != 1:
            raise ICHasExtraSheets(self.icReader.fileName)

        self.df = pd.read_excel(self.icReader.filePath, dtype=type("")) # read everything as a string

        if 'Amount' in self.df.columns.values: #If it is the new format
            self.df = self.getNewDf(self.df)

        # parse the file name and header information
        try:
            errorColumns = self.icReader.readBatch(list(self.df.columns.values))  # supply the header
            self.errorColumns = errorColumns
        except Exception as e:
            print("AN EXCEPTION!!")
            raise e

        # check for duplicates
        sqlBatch = "SELECT ic_batch_id FROM ic_batches WHERE date_run = ? AND " \
                   "project_id = ? AND operator = ? AND file_name = ?;"
        batchTuple = (self.icReader.runDate, self.icReader.projectId,
                       self.icReader.operator, self.icReader.fileName)

        self.cursor.execute(sqlBatch, batchTuple)
        batches = self.cursor.fetchall()


        if not self.uploader.allowDuplicates:
            if len(batches) > 0:
                raise DuplicateNotAllowed(self.icReader.fileName)


        # upload
        sqlUpload = "INSERT INTO ic_batches (date_run, project_id," \
                    "operator, file_name, file_path) VALUES (?,?,?,?,?);"
        uploadTuple = (self.icReader.runDate, self.icReader.projectId,
                       self.icReader.operator, self.icReader.fileName, self.icReader.filePath)
        self.cursor.execute(sqlUpload, uploadTuple)


        # retreive the batch id
        sqlBatch = "SELECT ic_batch_id FROM ic_batches WHERE date_run = ? AND " \
                   "project_id = ? AND operator = ? AND file_name = ?;"
        batchTuple = (self.icReader.runDate, self.icReader.projectId,
                       self.icReader.operator, self.icReader.fileName)

        self.cursor.execute(sqlBatch, batchTuple)
        currentBatches = self.cursor.fetchall()
        self.currentBatch = currentBatches[-1][0]

        #return errorColums

    def uploadCation(self):

        sqlCation = "INSERT INTO ic_cation_reads (sort_chem, batch_id, " \
                    "lithium, sodium, ammonium, potassium, magnesium, calcium, strontium) VALUES " \
                    "(?,?,?,?,?,?,?,?,?)"
        cationTuple = (
            self.icReader.cationSortChem,
            self.currentBatch,
            self.icReader.lithium,
            self.icReader.sodium,
            self.icReader.ammonium,
            self.icReader.potassium,
            self.icReader.magnesium,
            self.icReader.calcium,
            self.icReader.strontium
        )

        self.cursor.execute(sqlCation, cationTuple)

        # insert into the sort_chem_to_batch table
        sqlSort = "INSERT INTO sort_chems_to_ic_cation_batches (sort_chem, ic_batch_id) VALUES(?,?)"
        sortTuple = (self.icReader.cationSortChem, self.currentBatch)
        self.cursor.execute(sqlSort, sortTuple)


        # add to the sort-chem table
        # see if the sort chem is already in the sort-chems
        sqlCheck = "SELECT * FROM sort_chems WHERE sort_chem = ?;"
        checkTuple = (self.icReader.cationSortChem,)

        self.cursor.execute(sqlCheck, checkTuple)
        sortChems = self.cursor.fetchall()

        # if not, upload it
        if len(sortChems) == 0:
            sqlSort = "INSERT INTO sort_chems (sort_chem) VALUES (?);"
            sortTuple = (self.icReader.cationSortChem,)
            self.cursor.execute(sqlSort, sortTuple)

    def uploadAnion(self):

        sqlAnion = "INSERT INTO ic_anion_reads (sort_chem, batch_id, " \
                   "fluoride, acetate, formate, chloride, nitrite, bromide, nitrate, sulfate, phosphate) VALUES" \
                   " (?,?,?,?,?,?,?,?,?,?,?)"
        anionTuple = (
            self.icReader.anionSortChem,
            self.currentBatch,
            self.icReader.fluoride,
            self.icReader.acetate,
            self.icReader.formate,
            self.icReader.chloride,
            self.icReader.nitrite,
            self.icReader.bromide,
            self.icReader.nitrate,
            self.icReader.sulfate,
            self.icReader.phosphate
        )
        self.cursor.execute(sqlAnion, anionTuple)

        # insert into the sort_chem_to_batch table
        sqlSort = "INSERT INTO sort_chems_to_ic_anion_batches (sort_chem, ic_batch_id) VALUES(?,?)"
        sortTuple = (self.icReader.anionSortChem, self.currentBatch)
        self.cursor.execute(sqlSort, sortTuple)


        # see if the sort chem is already in the sort-chems
        sqlCheck = "SELECT * FROM sort_chems WHERE sort_chem = ?;"
        checkTuple = (self.icReader.anionSortChem,)

        self.cursor.execute(sqlCheck, checkTuple)
        sortChems = self.cursor.fetchall()

        # if not, upload it
        if len(sortChems) == 0:
            sqlSort = "INSERT INTO sort_chems (sort_chem) VALUES (?);"
            sortTuple = (self.icReader.anionSortChem,)
            self.cursor.execute(sqlSort, sortTuple)


    def uploadReads(self):

        problemOccured = False
        problemRows = []

        for index, row in self.df.iterrows(): # parse each row
            try:
                self.icReader.readRow(list(row))

                if self.icReader.rowContainsAnion:
                     self.uploadAnion()
                if self.icReader.rowContainsCation:
                     self.uploadCation()
            except:
                problemRows.append(int(index) + 2)
                problemOccured = True

        if problemOccured:
            raise ICDataError(self.icReader.fileName, problemRows)
        if len(self.errorColumns) > 0:
            message = "Warning: The following expected columns were missing from the headers of the IC sheet: " + str(self.errorColumns) + "\n\n"
            raise Warnings(message, self.icReader.fileName)

