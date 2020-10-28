import csv
from CustomErrors import *



class UploadScanFP:
    def __init__(self, cursor, uploader, scanFPReader):
        self.cursor = cursor
        self.uploader = uploader
        self.scanFPReader = scanFPReader
        self.df = None
        self.semicolon = False
        self.tab = False

    def uploadBatch(self):
        # open the file, get meta-data
        try:
            with open(self.scanFPReader.filePath, "r+") as csvFile:
                firstLine = csvFile.readline()
                if ";" in firstLine:
                    self.semicolon = True
                else:
                    self.tab = True
            csvFile.close()


            with open (self.scanFPReader.filePath) as csvFile:
                if self.tab:
                    reader = csv.reader(csvFile, delimiter="\t")
                    header = next(reader)
                    columns = next(reader)
                elif self.semicolon:
                    reader = csv.reader(csvFile, delimiter=";")
                    header = next(reader)
                    columns = next(reader)
        except:
            raise UnableToOpenFile(self.scanFPReader.fileName)
        # read the data
        self.scanFPReader.readBatch(header, columns)

        # check for duplicates
        sqlBatch = "SELECT scan_fp_batch_id FROM scan_fp_batches WHERE file_name = ? AND project_id = ? AND scan_instrument_id = ?;"
        batchTuple = (self.scanFPReader.fileName, self.uploader.getProjectId(), self.scanFPReader.spectrolyzer)

        self.cursor.execute(sqlBatch, batchTuple)
        batches = self.cursor.fetchall()

        if not self.uploader.allowDuplicates:
            if len(batches) > 0:
                raise DuplicateNotAllowed(self.scanFPReader.fileName)


        # upload the data
        sqlBatch = "INSERT INTO scan_fp_batches (file_name, file_path, project_id, scan_instrument_id) VALUES (?,?,?,?);"
        batchTuple = (self.scanFPReader.fileName, self.scanFPReader.filePath, self.uploader.getProjectId(),
                      self.scanFPReader.spectrolyzer)
        self.cursor.execute(sqlBatch, batchTuple)

        # retain the batch_id
        sqlBatch = "SELECT scan_fp_batch_id FROM scan_fp_batches WHERE file_name = ? AND project_id = ? AND scan_instrument_id = ?;"
        batchTuple = (self.scanFPReader.fileName, self.uploader.getProjectId(), self.scanFPReader.spectrolyzer)
        self.cursor.execute(sqlBatch, batchTuple)

        currentBatches = self.cursor.fetchall()

        self.currentBatch = currentBatches[-1][0]

    def uploadRow(self, row, i):
        result = self.scanFPReader.readRow(row)
        if result == self.scanFPReader.error:
            self.problemRows.append(i)
            return

        # check for duplicate datetime values
        sqlTime = "SELECT datetime_run FROM scan_fp_reads WHERE datetime_run = ?;"
        timeTuple = (self.scanFPReader.dateTimeValue,)

        self.cursor.execute(sqlTime, timeTuple)
        batches = self.cursor.fetchall()


        if (len(batches) > 0) and (not self.uploader.allowDuplicates):
            self.errorsOccured = True
            self.duplicateRows.append(i)
        else:

            # upload the row
            sqlRow = "INSERT INTO scan_fp_reads (scan_fp_batch_id, datetime_run, \
                    nm200,nm202,nm205,nm207,nm210,nm212,nm215,nm217,nm220,nm222, \
                    nm225,nm227,nm230,nm232,nm235,nm237,nm240,nm242,nm245,nm247, \
                    nm250,nm252,nm255,nm257,nm260,nm262,nm265,nm267,nm270,nm272, \
                    nm275,nm277,nm280,nm282,nm285,nm287,nm290,nm292,nm295,nm297, \
                    nm300,nm302,nm305,nm307,nm310,nm312,nm315,nm317,nm320,nm322, \
                    nm325,nm327,nm330,nm332, nm335,nm337,nm340,nm342,nm345,nm347,nm350, \
                    nm352,nm355,nm357,nm360,nm362,nm365,nm367,nm370,nm372,nm375, \
                    nm377,nm380,nm382,nm385,nm387,nm390,nm392,nm395,nm397,nm400, \
                    nm402,nm405,nm407,nm410,nm412,nm415,nm417,nm420,nm422,nm425, \
                    nm427,nm430,nm432,nm435,nm437,nm440,nm442,nm445,nm447,nm450, \
                    nm457,nm460,nm462,nm465,nm467,nm470,nm472,nm475,nm477,nm480, \
                    nm482,nm485,nm487,nm490,nm492,nm495,nm497,nm500,nm502,nm505, \
                    nm507,nm510,nm512,nm515,nm517,nm520,nm522,nm525,nm527,nm530, \
                    nm532,nm535,nm537,nm540,nm542,nm545,nm547,nm550,nm552,nm555, \
                    nm557,nm560,nm562,nm565,nm567,nm570,nm572,nm575,nm577,nm580, \
                    nm582,nm585,nm587,nm590,nm592,nm595,nm597,nm600,nm602,nm605, \
                    nm607,nm610,nm612,nm615,nm617,nm620,nm622,nm625,nm627,nm630, \
                    nm632,nm635,nm637,nm640,nm642,nm645,nm647,nm650,nm652,nm655, \
                    nm657,nm660,nm662,nm665,nm667,nm670,nm672,nm675,nm677,nm680, \
                    nm682,nm685,nm687,nm690,nm692,nm695,nm697,nm700,nm702,nm705, \
                    nm707,nm710,nm712,nm715,nm717,nm720,nm722,nm725,nm727,nm730, \
                    nm732,nm735,nm737,nm740,nm742,nm745,nm747,nm750) \
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, \
                    ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, \
                    ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, \
                    ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, \
                    ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, \
                    ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, \
                    ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, \
                    ?,?,?,?,?,?,?,?,?,?,?,?)"

            rowTuple = (self.currentBatch, self.scanFPReader.dateTimeValue,
                self.scanFPReader.value200,self.scanFPReader.value202,
                self.scanFPReader.value205,self.scanFPReader.value207,self.scanFPReader.value210,
                self.scanFPReader.value212,self.scanFPReader.value215,self.scanFPReader.value217,
                self.scanFPReader.value220,self.scanFPReader.value222,self.scanFPReader.value225,
                self.scanFPReader.value227,self.scanFPReader.value230,self.scanFPReader.value232,
                self.scanFPReader.value235,self.scanFPReader.value237,self.scanFPReader.value240,
                self.scanFPReader.value242,self.scanFPReader.value245,self.scanFPReader.value247,
                self.scanFPReader.value250,self.scanFPReader.value252,self.scanFPReader.value255,
                self.scanFPReader.value257,self.scanFPReader.value260,self.scanFPReader.value262,
                self.scanFPReader.value265,self.scanFPReader.value267,self.scanFPReader.value270,
                self.scanFPReader.value272,self.scanFPReader.value275,self.scanFPReader.value277,
                self.scanFPReader.value280,self.scanFPReader.value282,self.scanFPReader.value285,
                self.scanFPReader.value287,self.scanFPReader.value290,self.scanFPReader.value292,
                self.scanFPReader.value295,self.scanFPReader.value297,self.scanFPReader.value300,
                self.scanFPReader.value302,self.scanFPReader.value305,self.scanFPReader.value307,
                self.scanFPReader.value310,self.scanFPReader.value312,self.scanFPReader.value315,
                self.scanFPReader.value317,self.scanFPReader.value320,self.scanFPReader.value322,
                self.scanFPReader.value325,self.scanFPReader.value327,self.scanFPReader.value330, self.scanFPReader.value332,
                self.scanFPReader.value335,self.scanFPReader.value337,self.scanFPReader.value340,
                self.scanFPReader.value342,self.scanFPReader.value345,self.scanFPReader.value347,
                self.scanFPReader.value350,self.scanFPReader.value352,self.scanFPReader.value355,
                self.scanFPReader.value357,self.scanFPReader.value360,self.scanFPReader.value362,
                self.scanFPReader.value365,self.scanFPReader.value367,self.scanFPReader.value370,
                self.scanFPReader.value372,self.scanFPReader.value375,self.scanFPReader.value377,
                self.scanFPReader.value380,self.scanFPReader.value382,self.scanFPReader.value385,
                self.scanFPReader.value387,self.scanFPReader.value390,self.scanFPReader.value392,
                self.scanFPReader.value395,self.scanFPReader.value397,self.scanFPReader.value400,
                self.scanFPReader.value402,self.scanFPReader.value405,self.scanFPReader.value407,
                self.scanFPReader.value410,self.scanFPReader.value412,self.scanFPReader.value415,
                self.scanFPReader.value417,self.scanFPReader.value420,self.scanFPReader.value422,
                self.scanFPReader.value425,self.scanFPReader.value427,self.scanFPReader.value430,
                self.scanFPReader.value432,self.scanFPReader.value435,self.scanFPReader.value437,
                self.scanFPReader.value440,self.scanFPReader.value442,self.scanFPReader.value445,
                self.scanFPReader.value447,self.scanFPReader.value450,self.scanFPReader.value457,
                self.scanFPReader.value460,self.scanFPReader.value462,self.scanFPReader.value465,
                self.scanFPReader.value467,self.scanFPReader.value470,self.scanFPReader.value472,
                self.scanFPReader.value475,self.scanFPReader.value477,self.scanFPReader.value480,
                self.scanFPReader.value482,self.scanFPReader.value485,self.scanFPReader.value487,
                self.scanFPReader.value490,self.scanFPReader.value492,self.scanFPReader.value495,
                self.scanFPReader.value497,self.scanFPReader.value500,self.scanFPReader.value502,
                self.scanFPReader.value505,self.scanFPReader.value507,self.scanFPReader.value510,
                self.scanFPReader.value512,self.scanFPReader.value515,self.scanFPReader.value517,
                self.scanFPReader.value520,self.scanFPReader.value522,self.scanFPReader.value525,
                self.scanFPReader.value527,self.scanFPReader.value530,self.scanFPReader.value532,
                self.scanFPReader.value535,self.scanFPReader.value537,self.scanFPReader.value540,
                self.scanFPReader.value542,self.scanFPReader.value545,self.scanFPReader.value547,
                self.scanFPReader.value550,self.scanFPReader.value552,self.scanFPReader.value555,
                self.scanFPReader.value557,self.scanFPReader.value560,self.scanFPReader.value562,
                self.scanFPReader.value565,self.scanFPReader.value567,self.scanFPReader.value570,
                self.scanFPReader.value572,self.scanFPReader.value575,self.scanFPReader.value577,
                self.scanFPReader.value580,self.scanFPReader.value582,self.scanFPReader.value585,
                self.scanFPReader.value587,self.scanFPReader.value590,self.scanFPReader.value592,
                self.scanFPReader.value595,self.scanFPReader.value597,self.scanFPReader.value600,
                self.scanFPReader.value602,self.scanFPReader.value605,self.scanFPReader.value607,
                self.scanFPReader.value610,self.scanFPReader.value612,self.scanFPReader.value615,
                self.scanFPReader.value617,self.scanFPReader.value620,self.scanFPReader.value622,
                self.scanFPReader.value625,self.scanFPReader.value627,self.scanFPReader.value630,
                self.scanFPReader.value632,self.scanFPReader.value635,self.scanFPReader.value637,
                self.scanFPReader.value640,self.scanFPReader.value642,self.scanFPReader.value645,
                self.scanFPReader.value647,self.scanFPReader.value650,self.scanFPReader.value652,
                self.scanFPReader.value655,self.scanFPReader.value657,self.scanFPReader.value660,
                self.scanFPReader.value662,self.scanFPReader.value665,self.scanFPReader.value667,
                self.scanFPReader.value670,self.scanFPReader.value672,self.scanFPReader.value675,
                self.scanFPReader.value677,self.scanFPReader.value680,self.scanFPReader.value682,
                self.scanFPReader.value685,self.scanFPReader.value687,self.scanFPReader.value690,
                self.scanFPReader.value692,self.scanFPReader.value695,self.scanFPReader.value697,
                self.scanFPReader.value700,self.scanFPReader.value702,self.scanFPReader.value705,
                self.scanFPReader.value707,self.scanFPReader.value710,self.scanFPReader.value712,
                self.scanFPReader.value715,self.scanFPReader.value717,self.scanFPReader.value720,
                self.scanFPReader.value722,self.scanFPReader.value725,self.scanFPReader.value727,
                self.scanFPReader.value730,self.scanFPReader.value732,self.scanFPReader.value735,
                self.scanFPReader.value737,self.scanFPReader.value740,self.scanFPReader.value742,
                self.scanFPReader.value745,self.scanFPReader.value747,self.scanFPReader.value750)


            self.cursor.execute(sqlRow, rowTuple)


            # insert into the sort_chem_to_scan_fps table:
            sqlSort = "INSERT INTO scan_datetimes_to_scan_fp_batches (datetime_run,scan_fp_batch_id) VALUES (?,?);"
            sortTuple = (self.scanFPReader.dateTimeValue, self.currentBatch)

            self.cursor.execute(sqlSort, sortTuple)




    def uploadReads(self):
        self.errorsOccured = False
        self.duplicateRows = []
        self.problemRows = []

        with open(self.scanFPReader.filePath) as csvFile:
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

                self.uploadRow(row, i)


                i = i + 1
        if self.errorsOccured:
            raise DuplicateScanTimes(self.scanFPReader.fileName, self.duplicateRows)
        message = ""
        if len(self.duplicateRows) > 0:
            message = message + "ERROR: The following rows were duplicates of rows already present in the database " \
                                "and were not uploaded: " + str(self.duplicateRows) + ". If yo would like to upload them anyway," \
                                "please select \'allow duplicates\' above and resubmit.\n"
        if len(self.problemRows) > 0:
            message = message + "ERROR: The following rows were missing critical values and were therefore" \
                                " not uploaded: " + str(self.problemRows) + ".\n"
        if message != "":
            raise Warnings(message, self.scanFPReader.fileName)



