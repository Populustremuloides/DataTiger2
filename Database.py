from SenseFileOrigin import *
from Uploader import *
from EasterEggs import *
from CustomErrors import *
from QueryAnalyses import *
from DownloadData.DownloadTimeSeries import downloadStandardCurve
from DownloadData.DownloadTimeSeries import downloadTimeSeries
from DownloadData.DownloadTimeSeries import downloadLoggerGapsReport
import datetime
import os

class Database:
    def __init__(self):
        # connect to the database
        try:

            # open the db file
            filename = os.path.join(os.getcwd(), "DatabaseName.txt")
            print(filename)
            with open(filename, "r") as dbNameFile:
                dbName = dbNameFile.read()
                print(dbName)
                self.defaultDBFile = dbName
                if self.defaultDBFile.endswith(".db"):
                    self.conn = sqlite3.connect(self.defaultDBFile)
                    self.cursor = self.conn.cursor()
                    self.querier = QueryAnalyses(self.cursor)
                    self.uploader = Uploader(self)
                    self.databaseOpen = True
                else:
                    self.databaseOpen = False
        except:
            self.databaseOpen = False

        self.projectId = "Megafire"
        self.sensor = SenseFileOrigin()
        self.easterEggs = EasterEggs()
        self.allowDuplicates = False
        self.success = 0
        self.failure = -1
        self.sassCoefficient = 0


    def changeProjectId(self, newId):
        self.projectId = newId
        self.uploader = Uploader(self)


    def analyzeTests(self):
        if self.databaseOpen:
            self.querier.getSortChems()
            self.sortChemsWithBatches = self.querier.checkSortChemTests()
        else:
            self.sortChemsWithBatches = []

        return self.sortChemsWithBatches


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

    def getProjects(self):
        sqlProjects = "SELECT project_id FROM projects"
        if self.databaseOpen:
            try:
                self.cursor.execute(sqlProjects)
                return self.cursor.fetchall()
            except:
                return [("unable to open database",)]
        else:
            return [("unable to open database",)]

    def changeSassCoefficient(self, coefficient):
        self.sassCoefficient = coefficient

    def getProjectId(self):
        return self.projectId

    def senseFileOrigin(self, path):
        return self.sensor.senseFileOrigin(path)


    def changeDBFile(self, dbFile):
        if dbFile == "My name is Harry Potter." or "Harry" in dbFile:
            message = "Hello Harry Potter, my name is Tom Riddle"
            return message
        if dbFile == "Do you know anything about the Chamber of Secrets?" or "Chamber" in dbFile:
            message = "yes."
            return message
        if dbFile == "Can you tell me?" or "tell me" in dbFile:
            message = "no.\n\n\nBut I can show you."
            return message

        try:
            filename = os.path.join(os.getcwd(), "DatabaseName.txt")
            dbFile = dbFile.replace("file:///","")
            dbFile = dbFile.replace("/","\\")

            if os.path.exists(dbFile) and dbFile.endswith(".db"):
                with open(filename, "w") as dbNameFile:
                # clean up the file path

                    # reset the connection
                    self.conn = sqlite3.connect(dbFile)
                    self.cursor = self.conn.cursor()

                    # reset the querier
                    self.defaultDBFile = dbFile
                    self.uploader = Uploader(self)
                    self.querier = QueryAnalyses(self.cursor)

                    print(dbFile)
                    dbNameFile.writelines(dbFile)
                    self.databaseOpen = True

                    # return a success message
                    message = "Database File succesfully converted to " + str(dbFile) + "\n\nThis file will be the new default database for this computer.\n\n You clever cat."
                    return message
            else:
                message = "ERROR: unable to change database file to " + str(dbFile) + "\n\n"
                return message
        except:
            try:
                self.conn = sqlite3.connect(self.defaultDBFile)
                self.cursor = self.conn.cursor()
                message = "ERROR: unable to change database file to " + str(dbFile) + "\n\n"
                return message
            except:
                self.databaseOpen = False
                message = "ERROR: unable to change database file to " + str(dbFile) + "\n\n"
                return message
            # finally:
            #     return an error

    def getPermutations(self, numToChoose, indices, selections):

        numNeg = 0
        keepers = []
        for index in indices:
            if index < 0:
                numNeg += 1
            else:
                keepers.append(index)

        if numNeg == (len(indices) - numToChoose):
            return [keepers]
        else:
            for i in range(len(keepers)):
                value = keepers[i]
                keepers[i] = -1

                selection = self.getPermutations(numToChoose, keepers, selections)[0]
                if not selection in selections:
                    selections.append(selection)

                # put it back
                keepers[i] = value

        return selections

    def writeTests(self, outputFile):
        if self.databaseOpen:

            # all 4
            listOfMissings = []
            missingMasterScan = self.querier.missingScanDatetimes
            missingScanFP = self.querier.missingScanFps
            missingScanPar = self.querier.missingScanPars
            missingElementar = self.querier.missingElementars
            missingICCation = self.querier.missingIcCations
            missingICAnion = self.querier.missingIcAnions
            missingICP = self.querier.missingIcps
            missingAqualog = self.querier.missingAqualogs
            # missingDocs = self.querier.missingDocs
            # missingNo3s = self.querier.missingNo3s
            # missingWaters = self.querier.missingWaters
            # missingSrps = self.querier.missingSrps
            testsList = ["masterScan","scanFP","scanPar","toc","ic_cation","ic_anion","icp","aqualog"]#, "doc-isotopes","no3-isotopes","water-isotope","srp"]

            listOfMissings.append(missingMasterScan)
            listOfMissings.append(missingScanFP)
            listOfMissings.append(missingScanPar)
            listOfMissings.append(missingElementar)
            listOfMissings.append(missingICCation)
            listOfMissings.append(missingICAnion)
            listOfMissings.append(missingICP)
            listOfMissings.append(missingAqualog)
            # listOfMissings.append(missingDocs)
            # listOfMissings.append(missingNo3s)
            # listOfMissings.append(missingWaters)
            # listOfMissings.append(missingSrps)

            possibleMissings = list(range(1,len(listOfMissings) + 1))
            possibleMissings.reverse()
            alreadyWrittenSortChems = set([])

            keys = []
            values = []
            masterPermutations = []

            for numMissing in possibleMissings:
                permutations = self.getPermutations(numMissing, range(len(listOfMissings)), [])
                for permutation in permutations:
                    missingSorts = set(listOfMissings[permutation[0]])
                    missingSortsKey = ""
                    for test in permutation:
                        missingSorts = missingSorts.intersection(listOfMissings[test])
                        if missingSortsKey == "":
                            missingSortsKey = str(testsList[test])
                        else:
                            missingSortsKey = missingSortsKey + "-" + str(testsList[test])

                    # remove duplicates
                    # missingSorts = set(missingSorts)
                    # complement with already included ones
                    missingSorts = missingSorts.difference(alreadyWrittenSortChems)

                    keys.append(missingSortsKey)
                    missingSortsList = list(missingSorts)
                    missingSortsList.sort()
                    values.append(missingSortsList)
                    masterPermutations.append(permutation)
                    alreadyWrittenSortChems = alreadyWrittenSortChems.union(missingSorts)

            missingTestsDict = dict(zip(keys, values))
            missingTestsPermutationsDict = dict(zip(keys, masterPermutations))
            print(missingTestsPermutationsDict)
            outputFile.write("sortChem," + ",".join(testsList) + "\n")
            for key in missingTestsDict.keys():
                permutation = missingTestsPermutationsDict[key]
                suffix = ""
                for i in range(len(testsList)):
                    if i in permutation:
                        suffix = suffix + ",1"
                    else:
                        suffix = suffix + ","
                suffix = suffix + "\n"
                sortChems = missingTestsDict[key]
                for sortChem in sortChems:
                    line = str(sortChem) + suffix
                    outputFile.write(line)

            # outputFile.write("\n")
            # outputFile.write("\n")
            # outputFile.write("Missing Aqualog SortChems\n")
            # if len(self.querier.missingAqualogs) > 0:
            #     for sortChem in self.querier.missingAqualogs:
            #         outputFile.write(sortChem)
            #
            #         outputFile.write("\n")
            #
            # outputFile.write("\n")
            # outputFile.write("\n")
            # outputFile.write("Missing DOC-Isotopes SortChems\n")
            # if len(self.querier.missingDocs) > 0:
            #     for sortChem in self.querier.missingDocs:
            #         outputFile.write(sortChem)
            #         outputFile.write("\n")
            #
            # outputFile.write("\n")
            # outputFile.write("\n")
            # outputFile.write("Missing Elementar SortChems\n")
            # if len(self.querier.missingElementars) > 0:
            #     for sortChem in self.querier.missingElementars:
            #         outputFile.write(sortChem)
            #         outputFile.write("\n")
            #
            # outputFile.write("\n")
            # outputFile.write("\n")
            # outputFile.write("Missing MasterScan SortChems\n")
            # if len(self.querier.missingScanDatetimes) > 0:
            #     for sortChem in self.querier.missingScanDatetimes:
            #         outputFile.write(sortChem)
            #         outputFile.write("\n")
            #
            #
            # outputFile.write("\n")
            # outputFile.write("\n")
            # outputFile.write("Missing Scan-FP SortChems\n")
            # if len(self.querier.missingScanFps) > 0:
            #     for sortChem in self.querier.missingScanFps:
            #         outputFile.write(sortChem)
            #         outputFile.write("\n")
            #
            # outputFile.write("\n")
            # outputFile.write("\n")
            # outputFile.write("Missing Scan-Par SortChems\n")
            # if len(self.querier.missingScanPars) > 0:
            #     for sortChem in self.querier.missingScanPars:
            #         outputFile.write(sortChem)
            #         outputFile.write("\n")
            #
            # outputFile.write("\n")
            # outputFile.write("\n")
            # outputFile.write("Missing IC-Cation SortChems\n")
            # if len(self.querier.missingIcCations) > 0:
            #     for sortChem in self.querier.missingIcCations:
            #         outputFile.write(sortChem)
            #         outputFile.write("\n")
            #
            # outputFile.write("\n")
            # outputFile.write("\n")
            # outputFile.write("Missing IC-Anion SortChems\n")
            # if len(self.querier.missingIcAnions) > 0:
            #     for sortChem in self.querier.missingIcAnions:
            #         outputFile.write(sortChem)
            #         outputFile.write("\n")
            #
            # outputFile.write("\n")
            # outputFile.write("\n")
            # outputFile.write("Missing ICP SortChems\n")
            # if len(self.querier.missingIcps) > 0:
            #     for sortChem in self.querier.missingIcps:
            #         outputFile.write(sortChem)
            #         outputFile.write("\n")
            #
            #
            # outputFile.write("\n")
            # outputFile.write("\n")
            # outputFile.write("Missing Lachat SortChems\n")
            # if len(self.querier.missingLachats) > 0:
            #     for sortChem in self.querier.missingLachats:
            #         outputFile.write(sortChem)
            #         outputFile.write("\n")
            #
            # outputFile.write("\n")
            # outputFile.write("\n")
            # outputFile.write("Missing NO3-Isotopes SortChems\n")
            # if len(self.querier.missingNo3s) > 0:
            #     for sortChem in self.querier.missingNo3s:
            #         outputFile.write(sortChem)
            #         outputFile.write("\n")
            #
            # outputFile.write("\n")
            # outputFile.write("\n")
            # outputFile.write("Missing SRP SortChems\n")
            # if len(self.querier.missingSrps) > 0:
            #     for sortChem in self.querier.missingSrps:
            #         outputFile.write(sortChem)
            #         outputFile.write("\n")
            #
            # outputFile.write("\n")
            # outputFile.write("\n")
            # outputFile.write("Missing Water-Isotope SortChems\n")
            # if len(self.querier.missingWaters) > 0:
            #     for sortChem in self.querier.missingWaters:
            #         outputFile.write(sortChem)
            #         outputFile.write("\n")
            #
            #
            # outputFile.write("\n")
            # outputFile.write("\n")
            # outputFile.write("Sort Chems that have been found in various "
            #                  "data files but for which no SampleID entry has been made, "
            #                  "files the sort Chem has been mentioned in\n")
            # if len(self.querier.sortChemsMissingInstructions) > 0:
            #     for sortChem in self.querier.sortChemsMissingInstructions:
            #         outputFile.write(sortChem)
            #         outputFile.write(self.querier.getSortChemOrigins(sortChem, ","))
            #         outputFile.write("\n")
            #
            #
            # outputFile.write("\n")
            # outputFile.write("\n")
            # outputFile.write("Sort Chems that are set as \'ignored\':\n")
            # if len(self.querier.ignoredSortChems) > 0:
            #     for sortChem in self.querier.ignoredSortChems:
            #         outputFile.write(sortChem)
            #         outputFile.write("\n")

    def toString(self, list):
        newList = []
        for item in list:
            newList.append(str(item))
        return newList

    def writeStandardCurveReport(self, path, testDict, optionsDict):
        if path == "":
            return "no path selected. Standard curve report not downloaded.\n"
        # try:
        return downloadStandardCurve(path, testDict, optionsDict, self.cursor)
        # except:
        #     return "unable to download time series report"

    def writeTimeSeriesReport(self, path, testDict, optionsDict):
        if path == "":
            return "no path selected. time series report not downloaded.\n"
        #try:
        return downloadTimeSeries(path, testDict, optionsDict, self.cursor)
        #except:
        #    return "unable to download time series report"
    
    def writeLoggerGapsReport(self, path, testDict, optionsDict):
        if path == "":
            return "no path selected. Logger gaps report not downloaded.\n"
        # try:
        return downloadLoggerGapsReport(path, self.cursor, testDict, optionsDict)
        # except:
        #     return "unable to download logger gaps report"

    def writeMissingTestsReport(self, path):
        # if path == "":
        #     return "no path selected. Missing tests report not downloaded.\n"

        # try:
            self.analyzeTests()
            with open(path, "w+") as outputFile:
                if len(self.sortChemsWithBatches) == 0:
                    outputFile.write("Unable to access the database.\n")

                # for sortChemRow in self.sortChemsWithBatches:
                #     sortChemRow = self.toString(sortChemRow)
                #     outputFile.write(",".join(sortChemRow))
                #     outputFile.write("\n")

                # write the different tests
                self.writeTests(outputFile)
            outputFile.close()
            message = "Successfully downloaded report to " + path + "\n\n"
            return message
        # except:
        #     message = "ERROR: unable to download report.\n\n"
        #     return message


    def uploadFile(self, path):

        try:
            fileOrigin = self.senseFileOrigin(path)
            print(path)
            print("fileOrigin: " + fileOrigin)

            if fileOrigin == "ignore":
                raise IgnoreFile
            elif fileOrigin == "no_data":
                raise NoDataToParse(path)
            elif fileOrigin == "unrecognized":
                return "ERROR: file format not recognized. " + path + " not uploaded to database.\n\n" + self.easterEggs.pickRandomCondolence(self.sassCoefficient)
            elif fileOrigin == "excel_sampleID":
                return "ERROR: " + path + " must first be converted to a .csv file before uploading to DataTiger.\n\n" + self.easterEggs.pickRandomCondolence(self.sassCoefficient)
            elif fileOrigin == "causedException":
                return "ERROR: " + path + " caused an exception. The file is likely missing headers, or may not contain any data.\n\n" + self.easterEggs.pickRandomCondolence(self.sassCoefficient)
            else:
                if self.databaseOpen:
                    self.uploader.uploadFile(self.cursor, path, fileOrigin, self.allowDuplicates)
                else:
                    raise DatabaseNotOpen(path)
            if self.databaseOpen:
                self.conn.commit()
                return("Successfully uploaded " + path + self.easterEggs.pickRandomCongrats(self.sassCoefficient))
            else:
                raise DatabaseNotOpen(path)
        except DatabaseNotOpen as e:
            return (e.message + self.easterEggs.pickRandomCondolence(self.sassCoefficient))
        except Warnings as e:
            self.conn.commit()
            return (e.message + self.easterEggs.pickRandomCongrats(self.sassCoefficient))
        except ErrorInAqualogRows as e:
            self.conn.commit()
            return(e.message + self.easterEggs.pickRandomCongrats(self.sassCoefficient))
        except SortChemProblemRows as e:
            self.conn.commit()
            return (e.message + self.easterEggs.pickRandomCongrats(self.sassCoefficient))
        except HoboRowsError as e:
            self.conn.commit()
            return (e.message + self.easterEggs.pickRandomCongrats(self.sassCoefficient))
        except ICDataError as e:
            self.conn.commit()
            return (e.message + self.easterEggs.pickRandomCongrats(self.sassCoefficient))
        except ScanPARReadsError as e:
            self.conn.commit()
            return (e.message + self.easterEggs.pickRandomCongrats(self.sassCoefficient))
        except DuplicateScanTimes as e:
            self.conn.commit()
            return (e.message + self.easterEggs.pickRandomCongrats(self.sassCoefficient))
        except ProblemRowsDetected as e:
            self.conn.commit()
            return (e.message + self.easterEggs.pickRandomCongrats(self.sassCoefficient))
        except ElementarDuplicatesAndProblemsOccured as e:
            self.conn.commit()
            return (e.message + self.easterEggs.pickRandomCongrats(self.sassCoefficient))
        except ElementarDuplicateRowsOccured as e:
            self.conn.commit()
            return (e.message + self.easterEggs.pickRandomCongrats(self.sassCoefficient))
        except ElementarProblemRowsOccured as e:
            self.conn.commit()
            return (e.message + self.easterEggs.pickRandomCongrats(self.sassCoefficient))
        except BadHobo as e:
            self.conn.commit()
            return (e.message + self.easterEggs.pickRandomCongrats(self.sassCoefficient))

        except IgnoreFile as e:
            self.conn.rollback()
            message = "ERROR: file type for " + path + " recognized as irrelevant. File ignored.\n" + self.easterEggs.pickRandomCondolence(self.sassCoefficient)
            return message
        except Error as e:
            self.conn.rollback()
            message = "ERROR: " + path + " not uploaded because of the following errors:" + "\n" + e.message + self.easterEggs.pickRandomCondolence(self.sassCoefficient)
            return(message)

        # except:
        #     print("SQL command not executed.")

    def detuple(self, list):
        if len(list) == 2:
            return list[0]

        newList = []
        for item in list:
            newList.append(item[0])
        return newList

    def getMaxSortChem(self, sortChems):
        currentYear = str(datetime.date.today().year)
        try:
            numericSorts = []
            for sortChem in sortChems:
                containsLetter = False
                containsDash = False
                startsWithCurrentYear = False

                for letter in sortChem:
                    if letter.isalpha():
                        containsLetter = True
                    if letter == "-":
                        containsDash = True

                currentYear = str(datetime.date.today().year)
                if sortChem.startswith(currentYear):
                    startsWithCurrentYear = True

                if containsDash and startsWithCurrentYear and not containsLetter:
                    year, number = sortChem.split("-")
                    numericSort = int(year + number)
                    numericSorts.append(numericSort)

            maxSortChem = max(numericSorts)
            if str(maxSortChem).startswith(str(currentYear)):
                return maxSortChem
            else:
                return str(currentYear) + "0000"
        except:
            currentYear = str(datetime.date.today().year)
            return str(currentYear) + "0000"

    def getSortChemsOnDB(self):
        sqlSort = "SELECT sort_chem, MAX(datetime_uploaded) FROM sort_chems GROUP BY (sort_chem);"
        self.cursor.execute(sqlSort)
        presentSortChems = self.cursor.fetchall()
        presentSortChems = presentSortChems[0]
        return presentSortChems

    def getValidSortChems(self, sortChems):
        validSortChems = []
        i = 0
        for sortChem in sortChems:
            if self.validSortChem(sortChem):
                validSortChems.append(sortChem)
            i = i + 1
        return sortChems


    def newSortChems(self, fileName, howMany):

        if not self.databaseOpen:
            return "Unable to generate sort chems. Invalid database.\n\n"

        # open the output file
        try:
            # get the highest sort-chem on record
            sqlSort = "SELECT highest_sort_chems FROM generated_sort_chems;"
            self.cursor.execute(sqlSort)
            sortChems = self.cursor.fetchall()
            sortChems = self.detuple(sortChems)
            maxSortChem = self.getMaxSortChem(sortChems)
            # check to make sure there isn't already a larger sort chem on the database:
            presentSortChems = self.getSortChemsOnDB() # get the sort chems
            presentSortChems = self.detuple(presentSortChems) # get rid of the tuples
            presentSortChems = self.getValidSortChems(presentSortChems) # clean the values
            maxPresentSort = self.getMaxSortChem(presentSortChems)

            if int(maxPresentSort) > int(maxSortChem):
                maxSortChem = str(maxSortChem)
                maxPresentSort = str(maxPresentSort)
                return "ERROR: the largest sort-chem value found from files uploaded to the database (" + maxPresentSort[0:4] + "-" + maxPresentSort[4:] + ") was larger than the " \
                       "sortChem value in the \'generated_sort_chems\' table (" + maxSortChem[0:4] + "-" + maxSortChem[4:] + "). Please update " \
                       "the \'generated_sort_chems\' table to reflect the largest sort-chem known, then try again.\n\n"
            fileName = fileName + ".csv"
            with open(fileName, "w+") as output:
                # generate as many sort-chems as we need
                newSortChem = str(int(maxSortChem) + 1)
                for i in range(int(howMany)):

                    outputChem = str(newSortChem)[0:4] + "-" + str(newSortChem)[4:]
                    output.write(outputChem + "\n")
                    newSortChem = int(newSortChem) + 1
                newSortChem = int(newSortChem) - 1
                newSortChem = str(newSortChem)[0:4] + "-" + str(newSortChem)[4:]
            #
            sqlMax = "INSERT INTO generated_sort_chems (highest_sort_chems) VALUES (?);"
            maxTuple = (newSortChem,)

            self.cursor.execute(sqlMax, maxTuple)
            self.conn.commit()
            return "Success! " + str(howMany) + " new sort-chems generated and downloaded to " + fileName + ".csv.\n\n"

        except:
            return "ERROR: sort-chems were not succesfully generated or downloaded. The file you are trying to write to " \
                   "may be open in another program. Please close all instances of excel or another editor that have open the file" \
                   " name you designated. You may also be connected to a database that does not have a \'generated sort-chems\' table." \
                   " If this problem persists, please contact " \
                   "Brian Brown at bcbrown365@gmial.com.\n\n"






