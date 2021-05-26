import csv
from CustomErrors import *

class UploadScanPAR():

    def __init__(self, cursor, uploader, scanPARReader):
        self.cursor = cursor
        self.uploader = uploader
        self.scanPARReader = scanPARReader
        self.duplicateRows = []

        self.semicolon = False
        self.tab = False

    def uploadRow(self, row, i):
        try:
            self.scanPARReader.readRow(row)
        except:
            raise NoDataOnRow(self.scanPARReader.fileName)

        # check for duplicate datetime values
        sqlTime = "SELECT datetime_run FROM scan_par_reads WHERE datetime_run = ?;"
        timeTuple = (self.scanPARReader.datetimeValue,)

        self.cursor.execute(sqlTime, timeTuple)
        batches = self.cursor.fetchall()


        if (len(batches) > 0) and (not self.uploader.allowDuplicates):
            self.errorsOccurred = True
            self.duplicateRows.append(i)
        else:
            sqlRead = "INSERT INTO scan_par_reads (scan_par_batch_id, datetime_run, turbidity, no3, toc, doc) VALUES (?,?,?,?,?,?);"
            readTuple = (self.currentBatch, self.scanPARReader.datetimeValue, self.scanPARReader.turbidity,
                         self.scanPARReader.noc3, self.scanPARReader.toc, self.scanPARReader.doc)

            self.cursor.execute(sqlRead, readTuple)

        # insert into
        sqlSort = "INSERT INTO scan_datetimes_to_scan_par_batches (datetime_run,scan_par_batch_id) VALUES (?,?);"
        sortTuple = (self.scanPARReader.datetimeValue, self.currentBatch)

        self.cursor.execute(sqlSort, sortTuple)
    def uploadReads(self):
        self.errorsOccurred = False
        self.duplicateRows = []
        self.problemRows = []

        with open(self.scanPARReader.filePath) as csvFile:
            if self.tab:
                reader = csv.reader(csvFile, delimiter="\t")
            elif self.semicolon:
                reader = csv.reader(csvFile, delimiter=";")
            else:
                raise ScanFileDelimiterNotRecognized

            header = next(reader)
            columns = next(reader)

            i = 2
            for row in reader:
                try:
                    self.uploadRow(row, i)
                except NoDataOnRow:
                    self.problemRows.append(i)
                    self.errorsOccurred = True

                i = i + 1

            message = ""
            if len(self.problemRows) > 0:
                message = message + "ERROR: the following rows were not uploaded to the database because they " \
                                    "were missing critical values: " + str(self.problemRows) + "\n"
            if len(self.duplicateRows) > 0:
                message = message + "ERROR: the following rows were not uploaded to the database because they " \
                                    "were duplicates of scan.par entries with an identical datetime already present" \
                                    " ont the database: " + str(self.duplicateRows) + ". If you would like to " \
                                    "upload them to the database anyway, please select \'allow duplicates\'.\n"
            if message != "":
                raise ScanPARReadsError(message, self.scanPARReader.fileName)



    def uploadBatch(self):
        # open the file, get meta-data
        with open(self.scanPARReader.filePath, "r+") as csvFile:
            firstLine = csvFile.readline()[3:]
            if ";" in firstLine:
                self.semicolon = True
            else:
                self.tab = True
        csvFile.close()

        with open(self.scanPARReader.filePath) as csvFile:
            if self.tab:
                reader = csv.reader(csvFile, delimiter="\t")
                header = next(reader)
                columns = next(reader)
            elif self.semicolon:
                reader = csv.reader(csvFile, delimiter=";")
                header = next(reader)
                columns = next(reader)

        # read the data
        self.scanPARReader.readBatch(header, columns)

        # check for duplicates
        sqlBatch = "SELECT scan_par_batch_id FROM scan_par_batches WHERE file_name = ? AND project_id = ? AND scan_instrument_id = ?;"
        batchTuple = (self.scanPARReader.fileName, self.uploader.getProjectId(), self.scanPARReader.spectrolyzer)

        self.cursor.execute(sqlBatch, batchTuple)
        batches = self.cursor.fetchall()

        if not self.uploader.allowDuplicates:
            if len(batches) > 0:
                raise DuplicateNotAllowed(self.scanPARReader.fileName)


        # upload the data
        sqlBatch = "INSERT INTO scan_par_batches (file_name, file_path, project_id, scan_instrument_id) VALUES (?,?,?,?);"
        batchTuple = (self.scanPARReader.fileName, self.scanPARReader.filePath, self.uploader.getProjectId(),
                      self.scanPARReader.spectrolyzer)
        self.cursor.execute(sqlBatch, batchTuple)

        # retain the batch_id
        sqlBatch = "SELECT scan_par_batch_id FROM scan_par_batches WHERE file_name = ? AND project_id = ? AND scan_instrument_id = ?;"
        batchTuple = (self.scanPARReader.fileName, self.uploader.getProjectId(), self.scanPARReader.spectrolyzer)
        self.cursor.execute(sqlBatch, batchTuple)

        currentBatches = self.cursor.fetchall()

        self.currentBatch = currentBatches[-1][0]
