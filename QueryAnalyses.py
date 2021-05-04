import time
from CustomErrors import DatabaseTooLong

class QueryAnalyses:
    def __init__(self, cursor):
        self.cursor = cursor
        self.sortChems = []
        self.sortChemsMissingInstructions = []
        self.sortChemTests = []
        self.sortChemsToTests = {}

        self.aqualogIndex = 2
        self.docIndex = 3
        self.elementarIndex = 4
        self.scanIndex = 5
        self.icIndex = 6
        self.icpIndex = 7
        self.lachatIndex = 8
        self.no3Index = 9
        self.srpIndex = 10
        self.waterIndex = 11

        self.missingAqualogs = []
        self.missingDocs = []
        self.missingElementars = []
        self.missingScanDatetimes = []
        self.missingScanFps = []
        self.missingScanPars = []
        self.missingIcAnions = []
        self.missingIcCations = []
        self.missingIcps = []
        self.missingLachats = []
        self.missingNo3s = []
        self.missingSrps = []
        self.missingWaters = []

        self.ignoredSortChems = []

    def validSortChem(self, sortChem):
        numberDetected = False
        colonDetected = False
        spaceDetected = False
        slashDetected = False

        for c in sortChem:
            if c.isnumeric():
                numberDetected = True
            if c == ":":
                colonDetected = True
            if c == "/":
                slashDetected = True


        if numberDetected and not colonDetected and not spaceDetected and not slashDetected:
            if "ppm" in sortChem.lower():
                return False
            if "mg" in sortChem.lower():
                return False
            if "blank" in sortChem.lower():
                return False
            if "std" in sortChem.lower():
                return False
            return True
        else:
            return False

    def noInstructions(self, tuple):


        if tuple[self.aqualogIndex] == None:
            return True
        if tuple[self.docIndex] == None:
            return True
        if tuple[self.elementarIndex] == None:
            return True
        if tuple[self.scanIndex] == None:
            return True
        if tuple[self.icIndex] == None:
            return True
        if tuple[self.icpIndex] == None:
            return True
        if tuple[self.lachatIndex] == None:
            return True
        if tuple[self.no3Index] == None:
            return True
        if tuple[self.srpIndex] == None:
            return True
        if tuple[self.waterIndex] == None:
            return True
        return False

    def removeTuples(self, listOfTuples):
        cleanList = []
        for tuple in listOfTuples:
            cleanList.append(tuple[0])
        return cleanList

    def getTests(self, tuple):

        tests = []
        tests.append(tuple[self.aqualogIndex]) # aqualog_yes
        tests.append(tuple[self.docIndex]) # doc_isotopes_yes
        tests.append(tuple[self.elementarIndex]) # elementar_yes
        tests.append(tuple[self.scanIndex]) # scan_yes
        tests.append(tuple[self.icIndex]) # ic_yes
        tests.append(tuple[self.icpIndex]) # icp_yes
        tests.append(tuple[self.lachatIndex]) # lachat_yes
        tests.append(tuple[self.no3Index]) # no3_isotopes_yes
        tests.append(tuple[self.srpIndex]) # srp_yes
        tests.append(tuple[self.waterIndex]) # water_isotopes_yes

        return tests

    def getSortChems(self):
        # get all the sort chems:

        sqlSort = "SELECT sort_chem, ignore_yes, aqualog_yes, doc_isotopes_yes, elementar_yes, scan_yes, ic_yes, icp_yes, lachat_yes, no3_isotopes_yes, srp_yes, water_isotopes_yes, datetime_uploaded FROM sort_chems;"
        self.cursor.execute(sqlSort)

        rawTuples = self.cursor.fetchall()
        for tuple in rawTuples:
            # ignore those with an 'ignore' mark.

            if tuple[1] != 1 and self.validSortChem(tuple[0]):
                tuple = tuple[:-1] # remove the datetime uploaded

                # see if it has been decided which tests should be run for that sort-chem
                if self.noInstructions(tuple):
                    self.sortChemsMissingInstructions.append(tuple[0])
                else:
                    # see which tests the sort chem needs
                    tests = self.getTests(tuple)
                    self.sortChemsToTests[tuple[0]] = tests
            else:
                self.ignoredSortChems.append(tuple[0])

        self.sortChemsMissingInstructions = list(set(self.sortChemsMissingInstructions))
        self.ignoredSOrtChems = list(set(self.ignoredSortChems))

    def sampleIrrelevent(self, tuple):

        missing = []
        notRequested = []
        completed = []

        tests = tuple[1:]

        # check for letter answers
        for test in tests:
            test = str(test).lower()
            if "missing" in test:
                missing.append(test)
            elif "request" in test:
                notRequested.append(test)
            elif test.isdigit():
                completed.append(test)

        if len(missing) == 0:
            return True
        else:
            return False



    def getMax(self, list):

        max = -1
        for item in list:
            if item > max:
                max = item
        return max

    def returnFile(self, sqlFile, fileTuple):
        self.cursor.execute(sqlFile, fileTuple)
        result = self.cursor.fetchall()
        if len(result) > 0:
            return result[0][0]
        else:
            return ""

    def getAqualogFile(self, batchNumber):
        sqlFile = "SELECT file_name FROM aqualog_batches WHERE aqualog_batch_id = ?;"
        fileTuple = (batchNumber,)
        return self.returnFile(sqlFile, fileTuple)

    def getDocFile(self, batchNumber):
        sqlFile = "SELECT file_path FROM doc_isotope_batches WHERE doc_isotope_batch_id = ?;"
        fileTuple = (batchNumber,)
        return self.returnFile(sqlFile, fileTuple)

    def getElementarFile(self, batchNumber):
        sqlFile = "SELECT file_path FROM elementar_batches WHERE elementar_batch_id = ?;"
        fileTuple = (batchNumber,)
        return self.returnFile(sqlFile, fileTuple)

    def getScanMasterFile(self, batchNumber):
        sqlFile = "SELECT file_path FROM scan_master_batches WHERE scan_master_batch_id = ?;"
        fileTuple = (batchNumber,)
        return self.returnFile(sqlFile, fileTuple)

    def getScanParFile(self, batchNumber):
        sqlFile = "SELECT file_path FROM scan_par_batches WHERE scan_par_batch_id = ?;"
        fileTuple = (batchNumber,)
        return self.returnFile(sqlFile, fileTuple)

    def getScanFpFile(self, batchNumber):
        sqlFile = "SELECT file_path FROM scan_fp_batches WHERE scan_fp_batch_id = ?;"
        fileTuple = (batchNumber,)
        return self.returnFile(sqlFile, fileTuple)

    def getIcFile(self, batchNumber):
        sqlFile = "SELECT file_path FROM ic_batches WHERE ic_batch_id = ?;"
        fileTuple = (batchNumber,)
        return self.returnFile(sqlFile, fileTuple)

    def getIcpFile(self, batchNumber):
        sqlFile = "SELECT file_path FROM icp_batches_1 WHERE icp_batch_id = ?;"
        fileTuple = (batchNumber,)
        return self.returnFile(sqlFile, fileTuple)

    def getLachatFile(self, batchNumber):
        sqlFile = "SELECT file_path FROM lachat_batches WHERE lachat_batch_id = ?;"
        fileTuple = (batchNumber,)
        return self.returnFile(sqlFile, fileTuple)

    def getNo3IsotopesFile(self, batchNumber):
        sqlFile = "SELECT file_path FROM no3_batches WHERE no3_batch_id = ?;"
        fileTuple = (batchNumber,)
        return self.returnFile(sqlFile, fileTuple)

    def getSrpFile(self, batchNumber):
        sqlFile = "SELECT file_path FROM srp_batches WHERE srp_batch_id = ?;"
        fileTuple = (batchNumber,)
        return self.returnFile(sqlFile, fileTuple)

    def getWaterIsotopesFile(self, batchNumber):
        sqlFile = "SELECT file_path FROM water_isotopes_batches WHERE water_isotopes_batch_id = ?;"
        fileTuple = (batchNumber,)
        return self.returnFile(sqlFile, fileTuple)

    def getScanMasterBatch(self, sortChem):
        sqlBatch = "SELECT scan_master_batch_id from sort_chems_to_datetime_run WHERE sort_chem = ?"
        batchTuple = (sortChem,)
        self.cursor.execute(sqlBatch, batchTuple)

        return self.cursor.fetchall()

    def getSortChemOrigins(self, sortChem, delimiter):
        origins = ""
        # look at every test, and add the origins that match
        aqualogBatch = self.getMax(self.removeTuples(self.aqualogBatch(sortChem)))
        aqualogFile = self.getAqualogFile(aqualogBatch)
        if aqualogFile != "":
            origins = origins + delimiter + aqualogFile

        docBatch = self.getMax(self.removeTuples(self.docBatch(sortChem)))
        docFile = self.getDocFile(docBatch)
        if docFile != "":
            origins = origins + delimiter + docFile

        elementarBatch = self.getMax(self.removeTuples(self.elementarBatch(sortChem)))
        elementarFile = self.getElementarFile(elementarBatch)
        if elementarFile != "":
            origins = origins + delimiter + elementarFile

        scanMasterBatch = self.getMax(self.removeTuples(self.getScanMasterBatch(sortChem)))
        scanMasterFile = self.getScanMasterFile(scanMasterBatch)
        if scanMasterFile != "":
            origins = origins + delimiter + scanMasterFile

        scanFpBatch = self.getMax(self.removeTuples(self.scanFpBatch(sortChem)))
        scanFpFile = self.getScanFpFile(scanFpBatch)
        if scanFpFile != "":
            origins = origins + delimiter + scanFpFile

        scanParBatch = self.getMax(self.removeTuples(self.scanParBatch(sortChem)))
        scanParFile = self.getScanParFile(scanParBatch)
        if scanParFile != "":
            origins = origins + delimiter + scanParFile

        icAnionBatch = self.getMax(self.removeTuples(self.icAnionBatch(sortChem)))
        icAnionFile = self.getIcFile(icAnionBatch)
        if icAnionFile != "":
            origins = origins + delimiter + icAnionFile

        icCationBatch = self.getMax(self.removeTuples(self.icCationBatch(sortChem)))
        icCationFile = self.getIcFile(icCationBatch)
        if icCationFile != "":
            origins = origins + delimiter + icCationFile

        icpBatch = self.getMax(self.removeTuples(self.icpBatch(sortChem)))
        icpFile = self.getIcpFile(icpBatch)
        if icpFile != "":
            origins = origins + delimiter + icpFile

        lachatBatch = self.getMax(self.removeTuples(self.lachatBatch(sortChem)))
        lachatFile = self.getLachatFile(lachatBatch)
        if lachatFile != "":
            origins = origins + delimiter + lachatFile

        no3IsotopesBatch = self.getMax(self.removeTuples(self.no3Batch(sortChem)))
        no3IsotopesFile = self.getNo3IsotopesFile(no3IsotopesBatch)
        if no3IsotopesFile != "":
            origins = origins + delimiter + no3IsotopesFile

        srpBatch = self.getMax(self.removeTuples(self.srpBatch(sortChem)))
        srpFile = self.getSrpFile(srpBatch)
        if srpFile != "":
            origins = origins + delimiter + srpFile

        waterIsotopesBatch = self.getMax(self.removeTuples(self.waterBatch(sortChem)))
        waterIsotopesFile = self.getWaterIsotopesFile(waterIsotopesBatch)
        if waterIsotopesFile != "":
            origins = origins + delimiter + waterIsotopesFile

        return origins
    def checkSortChemTests(self):
        startTime = time.time()

        results = []
        key = ["sortChem","aqualog batch", "docIsotopes batch","elementar batch","scan datetime", "scan fp batch", "scan par batch","ic anion batch", "ic cation","icp batch","lachat batch","no3Isotopes batch","srp batch","waterIsotopes batch"]
        results.append(key)
        i = 0
        for sortChem in self.sortChemsToTests.keys():
            timeElapsed = time.time() - startTime
            if timeElapsed > 190:
                raise DatabaseTooLong()

            i = i + 1
            sortChemBatches = []
            sortChemBatches.append(sortChem)
            tests = self.sortChemsToTests[sortChem]

            # AQUALOG **************************************************************
            if tests[0] == 1:
                aqualogBatch = self.removeTuples(self.aqualogBatch(sortChem))
                if len(aqualogBatch) > 0:
                    aqualogBatch = self.getMax(aqualogBatch)
                else:
                    aqualogBatch = "Missing"
                    self.missingAqualogs.append(sortChem)
                sortChemBatches.append(aqualogBatch)

            else:
                sortChemBatches.append("Not Requested")
            # DOC ***********************************************************************
            if tests[1] == 1:
                docBatch = self.removeTuples(self.docBatch(sortChem))
                if len(docBatch) > 0:
                    docBatch = self.getMax(docBatch)
                else:
                    docBatch = "Missing"
                    self.missingDocs.append(sortChem)
                sortChemBatches.append(docBatch)
            else:
                sortChemBatches.append("Not Requested")

            # ELEMENTAR ******************************************************************88
            if tests[2] == 1:
                elementarBatch = self.removeTuples(self.elementarBatch(sortChem))

                if len(elementarBatch) > 0:
                    elementarBatch = self.getMax(elementarBatch)
                else:
                    elementarBatch = "Missing"
                    self.missingElementars.append(sortChem)
                sortChemBatches.append(elementarBatch)
            else:
                sortChemBatches.append("Not Requested")

            # SCAN INDEX ***********************************************************************
            if tests[3] == 1:
                datetime = self.scanMasterDatetime(sortChem)
                if len(datetime) > 0:
                    sortChemBatches.append(datetime)
                else:
                    sortChemBatches.append("Missing")
                    self.missingScanDatetimes.append(sortChem)

                scanFpBatch = self.removeTuples(self.scanFpBatch(datetime))
                if len(scanFpBatch) > 0:
                    scanFpBatch = self.getMax(scanFpBatch)
                else:
                    scanFpBatch = "Missing"
                    self.missingScanFps.append(sortChem)
                sortChemBatches.append(scanFpBatch)

                scanParBatch = self.removeTuples(self.scanParBatch(datetime))
                if len(scanParBatch) > 0:
                    scanParBatch = self.getMax(scanParBatch)
                else:
                    self.missingScanPars.append(sortChem)
                    scanParBatch = "Missing"
                sortChemBatches.append(scanParBatch)

            else:
                sortChemBatches.append("Not Requested")
                sortChemBatches.append("Not Requested")
                sortChemBatches.append("Not Requested")

            # IC *********************************************************************************
            if tests[4] == 1:
                icAnionBatch = self.removeTuples(self.icAnionBatch(sortChem))
                if len(icAnionBatch) > 0:
                    icAnionBatch = self.getMax(icAnionBatch)
                else:
                    self.missingIcAnions.append(sortChem)
                    icAnionBatch = "Missing"
                sortChemBatches.append(icAnionBatch)

                icCationBatch = self.removeTuples(self.icCationBatch(sortChem))
                if len(icCationBatch) > 0:
                    icCationBatch = self.getMax(icCationBatch)
                else:
                    self.missingIcCations.append(sortChem)
                    icCationBatch = "Missing"
                sortChemBatches.append(icCationBatch)
            else:
                sortChemBatches.append("Not Requested")
                sortChemBatches.append("Not Requested")

            # ICP *******************************************************************************
            if tests[5] == 1:
                icpBatch = self.removeTuples(self.icpBatch(sortChem))
                if len(icpBatch) > 0:
                    icpBatch = self.getMax(icpBatch)
                else:
                    self.missingIcps.append(sortChem)
                    icpBatch = "Missing"
                sortChemBatches.append(icpBatch)
            else:
                sortChemBatches.append("Not Requested")

            # LACHAT *****************************************************************************
            if tests[6] == 1:
                lachatBatch = self.removeTuples(self.lachatBatch(sortChem))
                if len(lachatBatch) > 0:
                    lachatBatch = self.getMax(lachatBatch)
                else:
                    self.missingLachats.append(sortChem)
                    lachatBatch = "Missing"
                sortChemBatches.append(lachatBatch)
            else:
                sortChemBatches.append("Not Requested")


            # NO3 *******************************************************************************
            if tests[7] == 1:
                no3Batch = self.removeTuples(self.no3Batch(sortChem))
                if len(no3Batch) > 0:
                    no3Batch = self.getMax(no3Batch)
                else:
                    self.missingNo3s.append(sortChem)
                    no3Batch = "Missing"
                sortChemBatches.append(no3Batch)
            else:
                sortChemBatches.append("Not Requested")

            # SRP *******************************************************************************
            if tests[8] == 1:
                srpBatch = self.removeTuples(self.srpBatch(sortChem))
                if len(srpBatch) > 0:
                    srpBatch = self.getMax(srpBatch)
                else:
                    self.missingSrps.append(sortChem)
                    srpBatch = "Missing"
                sortChemBatches.append(srpBatch)
            else:
                sortChemBatches.append("Not Requested")

            # WATER ****************************************************************************
            if tests[9] == 1:
                waterBatch = self.removeTuples(self.waterBatch(sortChem))
                if len(waterBatch) > 0:
                    waterBatch = self.getMax(waterBatch)
                else:
                    self.missingWaters.append(sortChem)
                    waterBatch = "Missing"
                sortChemBatches.append(waterBatch)
            else:
                sortChemBatches.append("Not Requested")

            # *********************************************************************************
            if not self.sampleIrrelevent(sortChemBatches):
                results.append(sortChemBatches)

        self.missingAqualogs = list(set(self.missingAqualogs))
        self.missingDocs = list(set(self.missingDocs))
        self.missingElementars = list(set(self.missingElementars))
        self.missingScanDatetimes = list(set(self.missingScanDatetimes))
        self.missingScanFps = list(set(self.missingScanFps))
        self.missingScanPars = list(set(self.missingScanPars))
        self.missingIcAnions = list(set(self.missingIcAnions))
        self.missingIcCations = list(set(self.missingIcCations))
        self.missingIcps = list(set(self.missingIcps))
        self.missingLachats = list(set(self.missingLachats))
        self.missingNo3s = list(set(self.missingNo3s))
        self.missingSrps = list(set(self.missingSrps))
        self.missingWaters = list(set(self.missingWaters))

        return results
        # for each sort-chem with an instruction sheet:
            # get the batch_id of each test that needs to be run (put NONE for those missing tests)
            # store in a list of lists

            # For each test:
            # store the sort_chems that have yet to be run for those tests


    def aqualogBatch(self, sortChem):
        sqlAqualog = "SELECT aqualog_batch_id FROM sort_chems_to_aqualog_batches WHERE sort_chem = ?;"
        aqualogTuple = (sortChem,)

        self.cursor.execute(sqlAqualog, aqualogTuple)
        return self.cursor.fetchall()

    def docBatch(self, sortChem):
        sqlDoc = "SELECT doc_isotope_batch_id FROM sort_chems_to_doc_isotope_batches WHERE sort_chem = ?;"
        docTuple = (sortChem,)

        self.cursor.execute(sqlDoc, docTuple)
        return self.cursor.fetchall()

    def elementarBatch(self, sortChem):
        sqlElementar = "SELECT elementar_batch_id FROM sort_chems_to_elementar_batches WHERE sort_chem = ?;"
        elementarTuple = (sortChem,)

        self.cursor.execute(sqlElementar, elementarTuple)
        return self.cursor.fetchall()

    def scanMasterDatetime(self, sortChem):
        sqlDatetime = "SELECT datetime_run FROM sort_chems_to_datetime_run WHERE sort_chem = ?;"
        datetimeTuple = (sortChem,)

        self.cursor.execute(sqlDatetime, datetimeTuple)
        # if that sort-chem existed on the scan

        result = self.cursor.fetchall()
        newResult = []
        if len(result) > 0:
            for item in result:
                newResult.append(item[0])
            result = newResult[-1]
        return result


    def scanFpBatch(self, datetime):

        if len(datetime) > 0:
            datetime = datetime

            sqlFp = "SELECT scan_fp_batch_id FROM scan_datetimes_to_scan_fp_batches WHERE datetime_run = ?;"
            fpTuple = (datetime,)

            self.cursor.execute(sqlFp, fpTuple)
            datetime = self.cursor.fetchall()

        return datetime

    def scanParBatch(self, datetime):

        if len(datetime) > 0:
            datetime = datetime
            sqlPar = "SELECT scan_par_Batch_id FROM scan_datetimes_to_scan_par_batches WHERE datetime_run = ?;"
            parTuple = (datetime,)

            self.cursor.execute(sqlPar, parTuple)
            datetime = self.cursor.fetchall()

        return datetime


    def icAnionBatch(self, sortChem):
        sqlIc = "SELECT ic_batch_id FROM sort_chems_to_ic_anion_batches WHERE sort_chem = ?;"
        icTuple = (sortChem,)

        self.cursor.execute(sqlIc, icTuple)
        return self.cursor.fetchall()

    def icCationBatch(self, sortChem):
        sqlIc = "SELECT ic_batch_id FROM sort_chems_to_ic_cation_batches WHERE sort_chem = ?;"
        icTuple = (sortChem,)

        self.cursor.execute(sqlIc, icTuple)
        return self.cursor.fetchall()

    def icpBatch(self, sortChem):
        sqlIcp = "SELECT icp_batch_id FROM sort_chems_to_icp_batches_1 WHERE sort_chem = ?;"
        icpTuple = (sortChem,)

        self.cursor.execute(sqlIcp, icpTuple)
        return self.cursor.fetchall()

    def lachatBatch(self, sortChem):
        sqlLachat = "SELECT lachat_batch_id FROM sort_chems_to_lachat_batches WHERE sort_chem = ?;"
        lachatTuple = (sortChem,)

        self.cursor.execute(sqlLachat, lachatTuple)
        return self.cursor.fetchall()

    def no3Batch(self, sortChem):
        sqlNo3 = "SELECT no3_batch_id FROM sort_chems_to_no3_batches WHERE sort_chem = ?;"
        no3Tuple = (sortChem,)

        self.cursor.execute(sqlNo3, no3Tuple)
        return self.cursor.fetchall()

    def srpBatch(self, sortChem):
        sqlSrp = "SELECT srp_batch_id FROM sort_chems_to_srp_batches WHERE sort_chem = ?;"
        srpTuple = (sortChem,)

        self.cursor.execute(sqlSrp, srpTuple)
        return self.cursor.fetchall()

    def waterBatch(self, sortChem):
        sqlWater = "SELECT water_isotopes_batch_id FROM sort_chems_to_water_isotopes_batches WHERE sort_chem = ?;"
        waterTuple = (sortChem,)

        self.cursor.execute(sqlWater, waterTuple)
        return self.cursor.fetchall()

