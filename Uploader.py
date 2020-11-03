from Readers.ReadEureka import *
import sqlite3

from Uploaders.UploadHanna import *
from Uploaders.UploadEureka import *
from Uploaders.UploadHobo import *
from Uploaders.UploadICP import *
from Uploaders.UploadIC import *
from Uploaders.UploadScanFP import *
from Uploaders.UploadScanPAR import *
from Uploaders.UploadScanMaster import *
from Uploaders.UploadElementar import *
from Uploaders.UploadSampleId import *
from Uploaders.UploadAqualog import *
from Uploaders.UploadDOCIsotopes import *
from Uploaders.UploadLachat import *
from Uploaders.UploadNo3 import *
from Uploaders.UploadSrp import *
from Uploaders.UploadWater import *
from Uploaders.UploadQ import *
from Uploaders.UploadSites import *
from Uploaders.UploadYSI import *


from Readers.ReadICP import *
from Readers.ReadIC import *
from Readers.ReadScanFP import *
from Readers.ReadScanPAR import *
from Readers.ReadScanMaster import *
from Readers.ReadElementar import *
from Readers.ReadSampleId import *
from Readers.ReadAqualog import *
from Readers.ReadDOCIsotopes import *
from Readers.ReadLachat import *
from Readers.ReadNo3 import *
from Readers.ReadSrp import *
from Readers.ReadWater import *
from Readers.ReadQ import *
from Readers.ReadSites import *
from Readers.ReadYSI import *

class Uploader:
    def __init__(self, database):
        self.hannaReader = None
        self.database = database

    def getProjectId(self):
        return self.database.getProjectId()

    def uploadFile(self, cursor, filePath, fileOrigin, allowDuplicates):
        self.allowDuplicates = allowDuplicates
        try:
            if fileOrigin == "field_hanna":

                self.hannaReader = ReadHanna(filePath)
                hannaUploader = UploaderHanna(cursor, self, self.hannaReader)

                # upload the data to the database
                hannaUploader.uploadHanna()
                hannaUploader.uploadBatch()
                hannaUploader.uploadLogs()

            if fileOrigin == "field_eureka": # *********************************************************

                self.eurekaReader = ReadEureka(filePath)
                self.eurekaUploader = UploaderEureka(cursor, self, self.eurekaReader)

                self.eurekaUploader.uploadEureka()
                self.eurekaUploader.uploadBatch()
                self.eurekaUploader.uploadLogs()

            if fileOrigin == "field_hobo.csv":
                self.hoboReader = ReadHobo(filePath)
                self.hoboUploader = UploadHobo(cursor, self, self.hoboReader, "absolute_pressure","hobo_logs", "hobo_batches")

                self.hoboUploader.uploadHobo()
                self.hoboUploader.uploadBatch()
                self.hoboUploader.uploadLogs()

            if fileOrigin == "light_hobo":
                self.hoboReader = ReadHobo(filePath)
                self.hoboUploader = UploadHobo(cursor, self, self.hoboReader, "intensity","hobo_light_logs", "hobo_light_batches")

                self.hoboUploader.uploadHobo()
                self.hoboUploader.uploadBatch()
                self.hoboUploader.uploadLogs()

            if fileOrigin == "dissolved_oxygen_hobo":
                self.hoboReader = ReadHobo(filePath)
                self.hoboUploader = UploadHobo(cursor, self, self.hoboReader, "dissolved_oxygen_mgl","hobo_oxygen_logs", "hobo_oxygen_batches")

                self.hoboUploader.uploadHobo()
                self.hoboUploader.uploadBatch()
                self.hoboUploader.uploadLogs()

            if fileOrigin == "conductivity_hobo":
                self.hoboReader = ReadHobo(filePath)
                self.hoboUploader = UploadHobo(cursor, self, self.hoboReader, "conductivity","hobo_conductivity_logs", "hobo_conductivity_batches")

                self.hoboUploader.uploadHobo()
                self.hoboUploader.uploadBatch()
                self.hoboUploader.uploadLogs()

            if fileOrigin == "field_hobo.hobo":
                raise RawHoboFileIngested(filePath)

            if fileOrigin == "icp":
                self.icpReader = ReadICP(filePath)
                self.icpUploader = UploadICP(cursor, self, self.icpReader)

                self.icpUploader.uploadBatch()
                self.icpUploader.uploadReads()

            if fileOrigin == "ic":
                self.icReader = ReadIC(filePath)
                self.icUploader = UploadIC(cursor, self, self.icReader)

                self.icUploader.uploadBatch()
                self.icUploader.uploadReads()
            if fileOrigin == "scan.fp":
                self.scanFPReader = ReadScanFP(filePath)
                self.scanFPUploader = UploadScanFP(cursor, self, self.scanFPReader)

                self.scanFPUploader.uploadBatch()
                self.scanFPUploader.uploadReads()
            if fileOrigin == "scan.par":
                self.scanPARReader = ReadScanPAR(filePath)
                self.scanPARUploader = UploadScanPAR(cursor, self, self.scanPARReader)

                self.scanPARUploader.uploadBatch()
                self.scanPARUploader.uploadReads()

            if fileOrigin == "masterScan":
                self.scanMasterReader = ReadScanMaster(filePath)
                self.scanMasterUploader = UploadScanMaster(cursor, self, self.scanMasterReader)

                self.scanMasterUploader.uploadBatch()
                self.scanMasterUploader.uploadReads()

            if fileOrigin == "elementar":
                self.elementarReader = ReadElementar(filePath)
                self.elementarUploader = UploadElementar(cursor, self, self.elementarReader)

                self.elementarUploader.uploadBatch()
                self.elementarUploader.uploadReads()

            if fileOrigin == "sampleID":
                self.sampleIdReader = ReadSampleId(filePath)
                self.sampleIdUploader = UploadSampleId(cursor, self, self.sampleIdReader)

                self.sampleIdUploader.uploadBatch()
                self.sampleIdUploader.uploadReads()

            if fileOrigin == "aqualog":
                self.aqualogReader = ReadAqualog(filePath)
                self.aqualogUploader = UploadAqualog(cursor, self, self.aqualogReader)

                self.aqualogUploader.uploadBatch()
                self.aqualogUploader.uploadReads()
            if fileOrigin == "docIsotopes":
                self.docReader = ReadDOCIsotopes(filePath)
                self.docUploader = UploadDOCIsotopes(cursor, self, self.docReader)

                self.docUploader.uploadBatch()
                self.docUploader.uploadReads()

            if fileOrigin == "lachat":
                self.lachatReader = ReadLachat(filePath)
                self.lachatUploader = UploadLachat(cursor, self, self.lachatReader)

                self.lachatUploader.uploadBatch()
                self.lachatUploader.uploadReads()

            if fileOrigin == "no3":
                self.no3Reader = ReadNo3(filePath)
                self.no3Uploader = UploadNo3(cursor, self, self.no3Reader)

                self.no3Uploader.uploadBatch()
                self.no3Uploader.uploadReads()

            if fileOrigin == "srp":
                self.srpReader = ReadSrp(filePath)
                self.srpUploader = UploadSrp(cursor, self, self.srpReader)

                self.srpUploader.uploadBatch()
                self.srpUploader.uploadReads()
            if fileOrigin == "water":
                self.waterReader = ReadWater(filePath)
                self.waterUploader = UploadWater(cursor, self, self.waterReader)

                self.waterUploader.uploadBatch()
                self.waterUploader.uploadReads()
            if fileOrigin == "q":
                self.qReader = ReadQ(filePath)
                self.qUploader = UploadQ(cursor, self, self.qReader)

                self.qUploader.uploadBatch()
                self.qUploader.uploadReads()
            if fileOrigin == "YSI":
                self.ysiReader = ReadYSI(filePath)
                self.ysiUploader = UploadYSI(cursor, self, self.ysiReader)

                self.ysiUploader.uploadBatch()
                self.ysiUploader.uploadReads()
            if fileOrigin == "sites":
                self.sitesReader = ReadSites(filePath)
                self.sitesUploader = UploadSites(cursor, self, self.sitesReader)

                self.sitesUploader.uploadBatch()
                self.sitesUploader.uploadReads()

        except sqlite3.OperationalError: # FIXME: PUT THIS BACK
            raise DatabaseLocked()
        #
        except Error as e:
            print("ERROR in UPLOADER")
            print(type(e))
            raise e

