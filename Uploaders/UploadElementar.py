from CustomErrors import *
import csv


class UploadElementar():
    def __init__(self, cursor, uploader, elementarReader):
        self.cursor = cursor
        self.uploader = uploader
        self.elementarReader = elementarReader
        self.problem = -1
        self.duplicate = -2

    def uploadRow(self):
        # check if the row already exists on the database
        sqlCheck = "SELECT * FROM elementar_reads WHERE date_run = ? " \
                   "AND time_run = ? AND hole = ? AND sort_chem = ?;"
        checkTuple = (self.elementarReader.date, self.elementarReader.time,
                      self.elementarReader.hole, self.elementarReader.sortChem)

        self.cursor.execute(sqlCheck, checkTuple)
        batches = self.cursor.fetchall()

        if (len(batches) > 0) and (not self.uploader.allowDuplicates):
            return self.duplicate

        # upload the batch # FIXME: make this so it returns -1 if it doesn't work
        sqlUpload = "INSERT INTO elementar_reads (elementar_batch_id," \
                    "hole, sort_chem," \
                    "method, tic_area, tc_area," \
                    "npoc_area, tnb_area, tic_mg_per_liter, tc_mg_per_liter," \
                    "npoc_mg_per_liter, tnb_mg_per_liter, date_run, time_run) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?);"
        uploadTuple = (self.currentBatch,
                       self.elementarReader.hole,
                       self.elementarReader.sortChem,
                       self.elementarReader.method,

                       self.elementarReader.ticArea,
                       self.elementarReader.tcArea,
                       self.elementarReader.npocArea,
                       self.elementarReader.tnbArea,

                       self.elementarReader.ticConcentration,
                       self.elementarReader.tcConcentration,
                       self.elementarReader.npocConcentration,
                       self.elementarReader.tnbConcentration,

                       self.elementarReader.date,
                       self.elementarReader.time)

        try:
            self.cursor.execute(sqlUpload, uploadTuple)



        except:
            return self.problem
            # # see if the sort chem is already in the sort-chems
            # sqlCheck = "SELECT * FROM sort_chems (sort_chem) WHERE sort_chem = ?;"
            # checkTuple = (self.elementarReader.sortChem,)
            #
            # self.cursor.execute(sqlCheck, checkTuple)
            # sortChems = self.cursor.fetchall()
            #
            # # if not, upload it
            # if len(sortChems) == 0:
            #     try:
            #         sqlSort = "INSERT INTO sort_chems (sort_chem) VALUES (?);"
            #         sortTuple = (self.elementarReader.sortChem,)
            #         self.cursor.execute(sqlSort, sortTuple)
            #     except:
            #         print("in here")

        # try:
        # see if the sort chem is already in the sort-chems
        sqlCheck = "SELECT * FROM sort_chems WHERE sort_chem = ?;"
        checkTuple = (self.elementarReader.sortChem,)

        self.cursor.execute(sqlCheck, checkTuple)
        sortChems = self.cursor.fetchall()

        # if not, upload it
        if len(sortChems) == 0:
            sqlSort = "INSERT INTO sort_chems (sort_chem) VALUES (?);"
            sortTuple = (self.elementarReader.sortChem,)
            self.cursor.execute(sqlSort, sortTuple)
        # except:
        #     print("in here")
        return 0

    def uploadSortChemToBatch(self):
        sqlSortChem = "INSERT INTO sort_chems_to_elementar_batches (sort_chem, elementar_batch_id) VALUES (?,?);"
        sortChemTuple = (self.elementarReader.sortChem, self.currentBatch)
        self.cursor.execute(sqlSortChem, sortChemTuple)

    def uploadSortChem(self):
        sqlUnique = "SELECT * FROM sort_chems WHERE sort_chem = ?;"
        uniqueTuple = (self.elementarReader.sortChem,)
        self.cursor.execute(sqlUnique, uniqueTuple)
        result = self.cursor.fetchall()

        if len(result) == 0:

            sqlSort = "INSERT INTO sort_chems (sort_chem) VALUES (?);"
            sortTuple = (self.elementarReader.sortChem,)
            self.cursor.execute(sqlSort, sortTuple)


    def uploadReads(self):
        self.problemsOccured = False
        self.duplicatesOccured = False
        self.problemRows = []
        self.duplicateRows = []
        with open(self.elementarReader.filePath) as csvFile:
            reader = csv.reader(csvFile, delimiter="\t")
            sepLine = next(reader)
            columns = next(reader)
            i = 0
            for row in reader:

                result = self.elementarReader.readRow(row)
                if result != 0:
                    self.problemsOccured = True
                    self.problemRows.append(i)

                else: # upload the row
                    result = self.uploadRow()
                    if result == self.duplicate:
                        self.duplicatesOccured = True
                        self.duplicateRows.append(i)
                    elif result == self.problem:
                        self.problemsOccured = True
                        self.problemRows.append(i)
                    else: # if it was a success
                        # add the sort-chem to batch
                        self.uploadSortChemToBatch()
                        self.uploadSortChem()



                i = i + 1

        if self.duplicatesOccured and self.problemsOccured:
            raise ElementarDuplicatesAndProblemsOccured(
            self.elementarReader.fileName, self.duplicateRows, self.problemRows)
        elif self.duplicatesOccured:
            raise ElementarDuplicateRowsOccured(self.elementarReader.fileName, self.duplicateRows)
        elif self.problemsOccured:
            raise ElementarProblemRowsOccured(self.elementarReader.fileName, self.problemRows)




    def uploadBatch(self):
        with open(self.elementarReader.filePath) as csvFile:
            reader = csv.reader(csvFile, delimiter="\t")
            sepLine = next(reader)
            columns = next(reader)

        # gather neccesary data
        self.elementarReader.readBatch(columns)

        # check for duplicates
        sqlBatch = "SELECT elementar_batch_id FROM elementar_batches WHERE date_run = ? AND " \
                   "project_id = ? AND operator = ? AND file_name = ?;"
        batchTuple = (self.elementarReader.runDate, self.elementarReader.projectId,
                       self.elementarReader.operator, self.elementarReader.fileName)

        self.cursor.execute(sqlBatch, batchTuple)
        batches = self.cursor.fetchall()

        if not self.uploader.allowDuplicates:
            if len(batches) > 0:
                raise DuplicateNotAllowed(self.elementarReader.fileName)


        # upload the batch
        sqlBatch = "INSERT INTO elementar_batches (date_run, project_id, operator, file_name, file_path) VALUES (?,?,?,?,?)"
        batchTuple = (
            self.elementarReader.runDate,
            self.elementarReader.projectId,
            self.elementarReader.operator,
            self.elementarReader.fileName,
            self.elementarReader.filePath
        )

        self.cursor.execute(sqlBatch, batchTuple)

        # retrieve the batch id
        sqlBatch = "SELECT elementar_batch_id FROM elementar_batches WHERE date_run = ? AND " \
                   "project_id = ? AND operator = ? AND file_name = ?;"
        batchTuple = (self.elementarReader.runDate, self.elementarReader.projectId,
                       self.elementarReader.operator, self.elementarReader.fileName)

        self.cursor.execute(sqlBatch, batchTuple)
        currentBatches = self.cursor.fetchall()
        self.currentBatch = currentBatches[-1][0]





