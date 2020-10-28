from Readers.ReadHanna import *
from CustomErrors import *

class UploaderHanna:
    def __init__(self, cursor, uploader, hannaReader):
        self.cursor = cursor
        self.uploader = uploader
        self.hannaReader = hannaReader
        self.currentBatch = None

        # separate the different excel sheets
        xls = pd.ExcelFile(self.hannaReader.getFilePath())
        self.sheets = xls.sheet_names
        # parse the information from the first sheet
        infoSheetName = self.sheets[0]
        info = pd.read_excel(self.hannaReader.getFilePath(), infoSheetName)
        result = self.hannaReader.readInfoSheet(info)
        if len(result) == 2:
            raise result[1]


    def getProjectId(self):
        return self.uploader.getProjectId()

    def uploadRow(self, hannaReader):

        self.hannaReader = hannaReader

        sqlInsert = """INSERT INTO hanna_reads 
        (logging_date, logging_time, temperature,
        pH, orp_mv, ec, pressure, do_percent, do_concentration, remarks, hanna_batch) 
        VALUES (?,?,?,?,?,?,?,?,?,?,?)"""

        insertTuple = (self.hannaReader.date, self.hannaReader.time,
                       self.hannaReader.temp, self.hannaReader.pH, self.hannaReader.orp,
                       self.hannaReader.ec, self.hannaReader.pressure, self.hannaReader.dissolvedOxygenPercent,
                       self.hannaReader.dissolvedOxygen, self.hannaReader.remarks, self.currentBatch)
        # try:
        self.cursor.execute(sqlInsert, insertTuple)
        # except:
        #     raise RowAlreadyUploadedError(self.hannaReader.fileName)

    def uploadBatch(self):
        # determine the site id
        if self.hannaReader.sitePrefix.isalpha():
            site_id = self.hannaReader.sitePrefix
        elif self.hannaReader.lotName.isalpha():
            site_id = self.hannaReader.lotName,
            if type(site_id) == type((12,)):
                site_id = site_id[0]
        else:
            raise siteIdNonDeterminable(self.hannaReader.fileName)

        # check for duplicates *********************************************
        sqlCheck = "SELECT * FROM hanna_batches WHERE hanna_serial_num = ? AND reference_temperature = ?" \
                   " AND temperature_coefficient = ? AND tds_factor = ? AND lot_name = ? AND start_date = ? " \
                   "AND start_time = ? AND samples_no = ? AND logging_interval = ? AND num_parameters = ? AND pc_software_version = ?" \
                   " AND site_id = ?"

        checkTuple = (
            self.hannaReader.serialNum,
            self.hannaReader.referenceTemp,
            self.hannaReader.temperatureCoefficient,
            self.hannaReader.tdsFactor,
            self.hannaReader.lotName,
            self.hannaReader.startDate,
            self.hannaReader.startTime,
            self.hannaReader.samplesNo,
            self.hannaReader.loggingInterval,
            self.hannaReader.numParameters,
            self.hannaReader.pcSoftwareVersion,
            site_id
        )


        self.cursor.execute(sqlCheck, checkTuple)
        batches = self.cursor.fetchall()

        if not self.uploader.allowDuplicates:
            if len(batches) > 0:
                raise DuplicateNotAllowed(self.hannaReader.getFileName())


        sqlInsert = """INSERT INTO hanna_batches (
        project_id, hanna_serial_num, reference_temperature, 
        temperature_coefficient, tds_factor, lot_name, remarks, start_date,
        start_time, samples_no, logging_interval,
        num_parameters, pc_software_version, site_id, file_name, file_path) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"""


        insertTuple = (self.getProjectId(), self.hannaReader.serialNum, self.hannaReader.referenceTemp,
                       self.hannaReader.temperatureCoefficient, self.hannaReader.tdsFactor,
                       self.hannaReader.lotName, self.hannaReader.remarks, self.hannaReader.startDate,
                       self.hannaReader.startTime, self.hannaReader.samplesNo, self.hannaReader.loggingInterval,
                       self.hannaReader.numParameters, self.hannaReader.pcSoftwareVersion, site_id, self.hannaReader.fileName,
                       self.hannaReader.filePath)
        try:
            self.cursor.execute(sqlInsert, insertTuple)
        except:
            raise batchAlreadyUploadedError(self.hannaReader.fileName)

        # get the batch id
        sqlGetBatchId = """SELECT hanna_batch_id FROM hanna_batches WHERE hanna_serial_num = ? AND 
            start_date = ? AND start_time = ?;"""
        getBatchIdTuple = (self.hannaReader.serialNum, self.hannaReader.startDate, self.hannaReader.startTime)

        self.cursor.execute(sqlGetBatchId, getBatchIdTuple)
        ids = self.cursor.fetchall()
        self.currentBatch = ids[-1][0]

    def uploadHanna(self):
        # check to see if the hanna serial number exists
        sqlChecker = """SELECT (hanna_serial_num) from hannas WHERE hanna_serial_num = ?;"""
        checkerTuple = (self.hannaReader.serialNum,)
        self.cursor.execute(sqlChecker, checkerTuple)
        matchingHanna = self.cursor.fetchall()

        if len(matchingHanna) == 0: # if the hanna doesn't exist on the database yet, add it

            sqlInsertHanna = """INSERT INTO hannas (hanna_serial_num,
                instrument_name, instrument_id, meter_software_version,
                meter_software_date) VALUES (?,?,?,?,?);"""

            hannaInsertTuple = (
                self.hannaReader.serialNum,
                self.hannaReader.instrumentName,
                self.hannaReader.instrumentId,
                self.hannaReader.meterSoftwareVersion,
                self.hannaReader.meterSoftwareDate
            )

            self.cursor.execute(sqlInsertHanna, hannaInsertTuple)

    def uploadLogs(self):
        # upload the logging data
        dataSheetName = self.sheets[1]
        data = pd.read_excel(self.hannaReader.getFilePath(), dataSheetName)

        headers = list(data.columns.values)
        self.hannaReader.readDataSheetHeaders(headers)

        repeatRows = []

        # go through every row
        for index, row in data.iterrows():
            # print(index)
            self.hannaReader.readDataSheetRow(data, index)
            if self.hannaReader.date != None:
                try: # handle redundant rows
                    self.uploadRow(self.hannaReader)
                except RowAlreadyUploadedError:
                    repeatRows.append(index)
        if len(repeatRows) > 0:
            raise SomeDataNotAddedError(str(repeatRows), self.hannaReader.fileName)

