import pandas as pd
import traceback
from CustomErrors import *

class upload_smartrock:
    def __init__(self, cursor, uploader, smartrock_reader):
        self.cursor = cursor
        self.uploader = uploader
        self.smartrock_reader = smartrock_reader
        self.batchNumber = None

        self.noErrors = 0
        self.error = 1

    def duplicateBatch(self):
        # see if the file name and project have already been uploaded
        sqlBatch = "SELECT * FROM smartrock_batches WHERE file_name = ?;"
        batchTuple = (self.smartrock_reader.fileName,)

        self.cursor.execute(sqlBatch, batchTuple)
        result = self.cursor.fetchall()

        if len(result) > 0:
            return True
        else:
            return False

    def getHeaders(self):
        first_col = self.df.iloc[:, 0]
        first_col = first_col.values.tolist()
        first_col.insert(0, self.df.columns.tolist()[0])

        for i in range(20):
            value = first_col[i].lower() if type(first_col[i]) == str else ""
            if "name" in value:
                headers = self.df.iloc[i-1].values.tolist()
                preheaders = self.df.columns.tolist()
                
                # reset self.df to include only values
                self.df = self.df.iloc[i:, :]

                index_of_localtime = preheaders.index('LocalTime')
                headers[index_of_localtime] = "logging_date"
                headers[index_of_localtime + 1] = "logging_time"

                # rename self.df to reflect headers rather than preheaders
                old_to_new_headers = {preheaders[i]: headers[i] for i in range(len(headers))}
                self.df = self.df.rename(columns=old_to_new_headers)

                self.smartrock_reader.readBatch(headers, preheaders)
        return

    def parse_date(self):
        # The following line pads datetimes with requisite zeros (ie, 2017-7-6 becomes 2017-07-06)
        self.df['datetime'] = self.df['logging_date'] + " " + self.df['logging_time']
        self.df['datetime'] = self.df['datetime'].apply(lambda x: " ".join(["/".join(list(map(lambda y: y.zfill(2), x.split(" ")[0].split("/")))),":".join(list(map(lambda y: y.zfill(2), x.split(" ")[1].split(":"))))]))
        self.df[['parsed_date', 'parsed_time']] = self.df.datetime.str.split(" ", expand=True)

    def uploadBatch(self):
        # check for duplicates
        if self.duplicateBatch() and not self.uploader.allowDuplicates:
            raise DuplicateBatch(self.smartrock_reader.fileName)
        else:
            self.df = pd.read_csv(self.smartrock_reader.filePath)
            self.getHeaders()
            self.parse_date()

            # upload the batch
            sqlBatch = "INSERT INTO smartrock_batches " \
                       "(project_id, file_name, file_path, datetime_uploaded) VALUES (?,?,?,?);"
            batchTuple = (self.smartrock_reader.projectId, self.smartrock_reader.fileName,
                          self.smartrock_reader.filePath, self.smartrock_reader.datetimeUploaded)

            self.cursor.execute(sqlBatch, batchTuple)

            # retrieve the batch id
            sqlBatch = "SELECT smartrock_batch_id FROM smartrock_batches WHERE datetime_uploaded = ? AND " \
                       "project_id = ? AND file_name = ?;"
            
            batchTuple = (self.smartrock_reader.datetimeUploaded, self.smartrock_reader.projectId,
                          self.smartrock_reader.fileName)

            self.cursor.execute(sqlBatch, batchTuple)
            currentBatches = self.cursor.fetchall()
            self.batchNumber = currentBatches[-1][0]

    def uploadReads(self):
        self.problemRows = []

        for index, row in self.df.iterrows():
            self.smartrock_reader.readRow(row)

            sqlRow = "INSERT INTO smartrock_reads (smartrock_batch_id, logging_date, logging_time, conductivity_uS_per_cm, turbidity_NTU, pressure, temperature_C) VALUES (?,?,?,?,?,?,?)"
            rowTuple = (self.batchNumber, self.smartrock_reader.date, self.smartrock_reader.time, self.smartrock_reader.ec, self.smartrock_reader.turbidity, self.smartrock_reader.pressure, self.smartrock_reader.temp)

            try:
                self.cursor.execute(sqlRow, rowTuple)
            except:
                print(traceback.format_exc())