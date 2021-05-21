

class Error(Exception):
    def __init__(self, message):
        self.message = message

class hannaPressureUnitNotRecognized(Error):
    def __init__(self, message):
        self.message = "Error: hanna pressure unit was not recognized. This is the header in teh original file: " + str(message) + " \n\n"

class hannaInfoSheetChanged(Error):
    def __init__(self, message):
        self.message = "Error: Hanna.xls sheet 1 has changed. See line " + str(message) + " on that sheet \n\n"

class errorProcessingHannaData(Error):
    def __init__(self, message):
        self.message = "ERROR: Hanna.xls sheet 2 has an error. See line " + str(message) + " on sheet 2 \n\n"

class siteIdNonDeterminable(Error):
    def __init__(self, message):
        self.message = "ERROR: the site ID was not determinable from either the " \
                       "file name or the \"lot name\" in the file " + str(message) + "\n\n"

class batchAlreadyUploadedError(Error):
    def __init__(self, message):
        self.message = "ERROR: the batch you just tried to upload already exists on the database. " \
                       "The data from " + message + " was therefore not uploaded. \n\n"
class RowAlreadyUploadedError(Error):
    def __init__(self, path):
        self.path = path
        self.message = "You shouldn't be accessing a message from this error, you little devil!\n\n"

class SomeDataNotAddedError(Error):
    def __init__(self, rows, path):
        self.message = "ERROR: some data in the file " + path + " was not added because " \
                       "the values already existed in the database. These rows include: " + str(rows) + "\n\n"

class EurekaInfoNotFound(Error):
    def __init__(self, path):
        self.message = "ERROR: the eureka file, " + path + " was not formatted correctly. DataTiger was unable " \
                        " to parse information about the Eureka from the file.\n\n"

class EurekaFileIncorrectlyNamed(Error):
    def __init__(self, path):
        self.message = "ERROR: the eureka file, " + path + " was incorrectly named. It is essential that the " \
                       "file be named with the SITEDATE.xls format. Please rename the file and submit again.\n\n"

class EurekaFileIncorrectlyFormated(Error):
    def __init__(self, path):
        self.message = "ERROR: the eureka file, " + path + " was not correctly formated. There was a row with an incorect number of commas.\n\n"

class RawHoboFileIngested(Error):
    def __init__(self, path):
        self.message = "ERROR: This tiger doesn't eat raw hobo files. Convert " + path + " to an .xls format and I'll be happy to take it off your hands.\n\n"
class NoDataToParse(Error):
    def __init__(self, path):
        self.message = "ERROR: " + path + " contained no parsable data. Unable to determine file type.\n\n"
class HoboSerialNumUnparsable(Error):
    def __init__(self, path):
        self.message = "ERROR: DataTiger normally parses the hobo serial number from the 3rd header on the second line. DataTiger was unable to do so in " + path + "\n\n"
class HoboIncorrectlyFormated(Error):
    def __init__(self, path):
        self.message = "ERROR: The hobo file, " + path + " was incorrectly formatted. DataTiger attempted to access logging data from the third row in the file but was unable " \
                                                         "to access the data\n\n"

class HoboMissingData(Error):
    def __init__(self, path, i):
        self.message = "ERROR: DataTiger was unable to upload data from " + path + " because the file was missing critical logging info at line " + str(i)

class HoboRowsError(Error):
    def __init__(self, path, rows):
        self.message = "WARNING: File successfully uploaded, but some data in the hobo .csv file " + path + " was not added. No data to parse on the following rows: " + str(rows)

class SiteNotInFileName(Error):
    def __init__(self, path):
        self.message = "ERROR: unable to upload " + path + ". The site id MUST be included as part of the file name for this file type (hobo).\n\n"

class DatabaseLocked(Error):
    def __init__(self):
        print('here')
        self.message = "ERROR: You or another user has recently altered the database and has not yet saved the file. This may mean that another user" \
                       "currently has DataTiger open, or it may mean that another user has made edits using the SQLite Database Browser tool and has" \
                       "not yet closed the database file. In either case, the problem can be solved by closing all other progrmas that are accessing the" \
                       "database file. In order to avoid data corruption, the database will remain locked until the file is saved and closed.\n\n"

class DuplicateNotAllowed(Error):
    def __init__(self, path):
        self.message = "ERROR: unable to upload " + path + " because a duplicate batch with matching meta-data already exists on the database," \
                      "and according to the settings, duplicates are not allowed. If you are trying to submit a file with data that has been corrected, then " \
                      "make sure to hit the \' allow duplicates\' button above before re-submitting the file.\n\n"

class BadHobo(Error):
    def __init__(self, path):
        self.message = "ERROR: the file " + path + " was recognized to contain faulty data. No logging data were uploaded.\n\n"

class ICHasExtraSheets(Error):
    def __init__(self, path):
        self.message = "ERROR: unable to upload " + path + " because the excel file contained multiple sheets, and DataTiger can only parse " \
                       "IC  and ICP files with one sheet. Please save the file with just one sheet, and re-upload. If you are unsure which sheet" \
                       " to use, please use the sheet that most closely matches the original, raw excel file.\n\n"


class ICRowError(Error):
    def __init__(self):
        self.message = "ERROR: there was a row in an ic file that was not correctly formatted. But this error message should never have been printed to the screen" \
                       "and something is wrong with the code. You should contact Brian Brown at bcbrown365@gmail.com.\n\n"
class ICDataError(Error):
    def __init__(self, path, problemRows):
        self.message = "WARNING: the following rows in " + path + " were not added to the " \
                      "database because they were not formatted correctly (likely missing some " \
                      "critical value). " + str(problemRows) + " The remaining rows in the file " \
                       "were successfully uploaded.\n\n"

class ICFileHasNoLabels(Error):
    def __init__(self, path):
        self.message = "ERROR: the file " + path + " had no labels to identify 'cation injection date' or 'anion injection date'. The parsing mechanism for ic files" \
                       " depends on these columns being present (even if they are blank). NOTE: these columns must be located" \
                       " immediately to the right of the name column. Pleaes insert these columns and try again. And do it soon! I'm hungry!\n\n"

class ICPDataError(Error):
    def __init__(self, path, problemRows):
        self.message = "ERROR: unable to upload rows " + str(problemRows) + " from " + str(path) + " because these rows were missing critical values.\n\n"

class ICPFormatChanged(Error):

    def __init__(self, path):
        self.message = "ERROR: the file " + path + " was not added to the database because the file format has changed. Likely the data must" \
                       "start within the first 20 rows.\n\n"
class ICPMustHaveDilutionColumn(Error):
    def __init__(self, path):
        self.message = "ERROR: the file " + path + " was not uploaded to the database. DataTiger was not able to find a \"dilution\" column, which " \
                                                   "is essential for DataTiger to properly parse the file. please add a dilution column immediately " \
                                                   "after the last column you want uploaded. (it is okay if the column is blank.)\n\n"
class IgnoreFile(Error):
    def __init__(self):
        self.message = "File ignored.\n\n"

class DuplicateScanTimes(Error):
    def __init__(self, path, rows):
        self.message = "WARNING: " + path + " was successfully added to the database, however, the rows " + str(rows) + " were not added to the" \
                        "database because they had date-time values that are already present in the database. " \
                        "If the number of rows that were not added is large compared to the file, then likely this file contained duplicated " \
                        "information already present in the database. However, there is a small possibility that the conflicting rows actually " \
                        " are not duplicates. This could result if two different scan machines took measurements on two different samples at exactly " \
                        " the same time. In this case, or in the case of adding corrected data under a different file name, please resubmit the " \
                        "file individually, but select the \'allow duplicates\' button above before doing so."

class ScanFileDelimiterNotRecognized(Error):
    def __init__(self, path):
        self.message = "ERROR: " + path + " was not uploaded to the database because the file separates columns with an unrecognized delimiter. DataTiger" \
                       "can recognize files with tab delimiters and semicolon delimiters.\n\n"

class ICPFileNotNamedCorrectly(Error):
    def __init__(self, path):
        self.message = "ERROR: " + path + " was not uploaded to the database because the file was not properly named. The file must " \
                          "be named according to the following convention: year.project.operator.xlsx. Please rename the file and try again.\n\n"

class NewSRPFileNotNamedCorrectly(Error):
    def __init__(self, path):
        self.message = "ERROR: " + path + " was not uploaded to the database because the file was not properly named. The file must " \
                                          "be named according to the following convention: date.project.xlsx. Please rename the file and try again.\n\n"

class NoDataOnRow(Error):
    def __init__(self, path):
        self.message = "ERROR: this message should not display to screen.\n\n"

class ProblemRowsDetected(Error):
    def __init__(self, path, rows):
        self.message = "Warming: " + path + " was successfully uploaded to the database, however, several rows were not able to " \
                            "be parsed. These rows included: " + str(rows) + " and likely were missing data on essential columns.\n\n"


class FileTypeNotRecognized(Error):
    def __init__(self, path):
        self.message = "ERROR: file format not recognized. " + path + " not uploaded to database.\n\n"

class ElementarFileNotNamedCorrectly(Error):
    def __init__(self, path):
        self.message = "ERROR: " + path + " was not uploaded to the database because the file was not properly named. The file is an " \
                          " Elementar file and must " \
                          "be named according to the following convention: year.proejct.operator.csv. Please rename the file and try again.\n\n"
class ElementarFileIncorrectlyFormated(Error):
    def __init__(self, path):
        self.message = "ERROR: " + path + " was not uploaded to the database because the file was not formatted correctly. DataTiger was not able" \
                          " to correctly identify all the columns it needed to parse. These columns must be located on the second row.\n\n"

class ElementarDuplicatesAndProblemsOccured(Error):
    def __init__(self, path, duplicateRows, problemRows):
        self.message = "Warning: the following rows in " + path + " were not added to the database because they were " \
                     "duplicates of rows already present in the database: " + str(duplicateRows) + ". In " \
                     "addition, the following rows were unable to be added to the database because they contained " \
                     "null values where there should have been data: " + str(problemRows) + ". The rest of the rows in the" \
                     "file were successfully added to the database.\n\n"

class ElementarDuplicateRowsOccured(Error):
    def __init__(self, path, duplicateRows):
        self.message = "WARNING: the following rows in " + path + " were not added to the database because they were " \
                      "duplicates of rows already present in the database: " + str(duplicateRows) + ". If you would like" \
                    " these rows to be added anyway, please select the \'allow duplicates\' button before re-uploading the file." \
                    " However, this will also duplicate the rows that were just uploaded to the database.\n\n"
class ElementarProblemRowsOccured(Error):
    def __init__(self, path, problemRows):
        self.message = "WARNING: the following rows in " + path + " were not uploaded to the database because they had missing" \
                        " values in the columns that DataTiger adds to the database:, and null values are not allowed for \'hole\'" \
                        "\'name\' or \'method\' columns: " + str(problemRows)

class UnableToOpenFile(Error):
    def __init__(self, path):
        self.message = "ERROR: Unable to open " + path + "\n\n"

class UnableToParseFile(Error):
    def __init__(self, path):
        self.message = "ERROR: unable to parse " + path + ". Likley the file did not contain enough rows to parse headers/information from.\n\n"

class MissingColumn(Error):
    def __init__(self, path):
        self.message = "ERROR: Columns missing in the sampleID file. " + path + " not uploaded.\n\n"

class SortChemProblemRows(Error):
    def __init__(self, message, path):
        self.message = "WARNING: " + path + " was uploaded correctly, but the following errors occured:\n" + message

class AqualogMissingColumns(Error):
    def __init__(self, path):
        self.message = "ERROR: unable to parse aqualog file " + path + " because it was missing the anticipated column headers.\n\n"

class ErrorInAqualogRows(Error):
    def __init__(self, message, path):
        self.message = "WARNING: " + path + " was uploaded correctly, but the following errors occured:\n" + message


class DuplicateDOCBatch(Error):
    def __init__(self, path):
        self.message = "ERROR: unable to upload " + path + " because an identically named file has already been uploaded" \
                       " to the database. If you would like to submit the file anyway, please select \'allow duplicates\' " \
                       " and submit again.\n\n"


class Warnings(Error):
    def __init__(self, message, path):
        self.message = "WARNING: " + path + " was uploaded correctly, but the following errors occured:\n" + message


class LachatNotFormattedCorrectly(Error):
    def __init__(self, path):
        self.message = "ERROR: unable to upload or parse " + path + " because it did not contain a \'sample id\' column\'" \
                         " in the first column, or another critical column was missing.\n\n"
class No3NotFormattedCorrectly(Error):
    def __init__(self, path):
        self.message = "ERROR: unable to upload or parse " + path + " because it did not " \
                        "contain a \'d18OVSMOW\' column\' in the third column, or " \
                        "another critical column was missing.\n\n"

class DuplicateBatch(Error):
    def __init__(self, path):
        self.message = "ERROR: unable to upload " + path + " because another identically named file already exists on the " \
                       "database. If you would like to upload the file anyway, please select \'allow duplicates\' above " \
                       "and resubmit.\n\n"
class ScanPARReadsError(Error):
    def __init__(self, message, path):
        self.message = "WARNING: The file " + path + " was succesfully uploaded to the database. However, the following " \
                     "errors occured:\n" + message
class DatabaseTooLong(Error):
    def __init__(self):
        pass

class DatabaseNotOpen(Error):
    def __init__(self, path):
        self.message = "ERROR: unable to upload " + path + " because the current database is invalid.\n\n"

class ICMissingNames(Error):
    def __init__(self, path):
        self.message = "ERROR: unable to upload " + path + " because it was missing the critical 'name' column before either the anion and/or cation columns (it needs a name column before each).\n\n"

class BlankError(Error):
    def __init__(self, message, path):
        self.message = message + "\n\n"


