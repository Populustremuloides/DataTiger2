import csv
import openpyxl
import traceback

from Readers.ReadHanna import *
from CustomErrors import *

import pandas as pd

class SenseFileOrigin():
    def isQ(self, filePath, sheetNames):
        exl = pd.ExcelFile(filePath)
        df = pd.read_excel(exl, sheetNames[-1])

        if df.shape[0] < 4:
            message = "ERROR: the file " + filePath + " did not contain sufficient information to parse (less than 4 rows (less than 4 rows).\n"
            raise Warnings(message, filePath)

        hannaReader = ReadHanna(filePath)
        # try:
        if True:
            hannaReader.readDataSheetRow(df,3)
            time1 = hannaReader.time
            second1 = int(time1.split(":")[-1])
            minute1 = int(time1.split(":")[-2])
            hour1 = int(time1.split(":")[-3])
        # except:
        #     message = "ERROR: Hanna file missing critical headers.\n"
        #     raise Warnings(message, filePath)

        hannaReader.readDataSheetRow(df,4)
        time2 = hannaReader.time
        second2 = int(time2.split(":")[-1])
        minute2 = int(time2.split(":")[-2])
        hour2 = int(time2.split(":")[-3])

        if hour2 - hour1 != 0:
            minute1 = minute1 + (60 * (hour2 - minute1))

        if minute2 - minute1 != 0:
            second2 = second2 + (60 * (minute2 - minute1))

        interval = second2 - second1

        if interval != 0 and interval < 10:
            return True
        else:
            return False

    def senseFileOrigin(self, filePath):
        # try:
        # check file handle:

        # if filePath.endswith(".xls"):
        #     df = pd.ExcelFile(filePath)
        #     sheetNames = df.sheet_names
        #     if len(sheetNames) == 2:
        #         return "field_hanna"
            if filePath.endswith(".xlsx") or filePath.endswith(".xls") or filePath.endswith(".XLS"):
                exl = pd.ExcelFile(filePath)
                sheetNames = exl.sheet_names
                df = pd.read_excel(exl, sheetNames[0])

                columns = list(df.columns.values)
                print(columns)

                if "#" == str(columns[0]):
                    return "sites"

                if "Sample ID" in str(columns[0]):
                    if "Site" in str(columns[1]):
                        return "water"

                if "Sortchem" in str(columns[0]):
                    return "srp"

                if "sortchem" in str(columns[0]):
                    if "Site" in str(columns[1]):
                        if ("sample" in str(columns[4])) and ("volume" in str(columns[4])):
                            return "tss"

                if "Operator" in str(columns[0]):
                    return "aqualog"

                if "Sample Identifier" in str(columns[0]):
                    return "docIsotopes"

                if "Chem #" in str(columns[0]):
                    return "masterScan"

                if len(columns) > 3:
                    if "OVSM" in str(columns[3]):
                        return "no3"
                i = 0
                while i < 15 and i < len(list(df.iloc[:,0])): # read through the first row
                    value = str(df.iloc[i,1]).lower()
                    if "no" in value:
                        return "lachat"
                    if "selected" in value and "peak" in value:
                        return "ic"
                    if "lab" in value and "#" in value and "Water" in sheetNames and not 'date' in str(df.iloc[i,0]).lower():
                        # If any instance of 'icp' exists in the line above "Lab #," return 'icp'. Else, return 'srp_new'
                        preheader = df.iloc[i-1].values.tolist()

                        # this checks for hidden/non hidden columns
                        workbook = openpyxl.load_workbook(filename=filePath)
                        lookhere = workbook.active.column_dimensions.items()

                        hidden_cols = []
                        unhidden_cols = []

                        # the following code converts Excel column names ('A', 'B', ... 'AB', 'AC', ...) to indices that are used to access the preheaders of df
                        for column, col_dimension in workbook.active.column_dimensions.items():
                            if col_dimension.hidden:
                                letter_list = [ord(x) - 97 for x in column.lower()]
                                number = (26 * (len(letter_list) - 1)) + letter_list[-1]

                                try:
                                    hidden_cols.append(df.iloc[i - 1, number])
                                except:
                                    print(traceback.format_exc())
                                    hidden_cols.append(None)

                            else:
                                letter_list = [ord(x) - 97 for x in column.lower()]
                                number = (26 * (len(letter_list) - 1)) + letter_list[-1]

                                try:
                                    unhidden_cols.append(df.iloc[i - 1, number])
                                except:
                                    print(traceback.format_exc())
                                    unhidden_cols.append(None)

                        if any(['icp' in str(x).lower() for x in unhidden_cols]):
                            return 'icp'
                        else:
                            return "srp_new"
                    elif "lab" in value and "#" in value and "Water" in sheetNames and 'date' in str(df.iloc[i,0]).lower():
                        return "icp"

                    i = i + 1

                i = 0
                while i < 10 and i < len(list(df.iloc[:,0])): # read through the first row

                    value = str(df.iloc[i,0]).lower()

                    if "instrument name" in value:
                        if len(sheetNames) == 2:
                            if self.isQ(filePath, sheetNames):
                                return "q"
                            else:
                                return "field_hanna"

                    if "name" in value:
                        return "icp"
                    if "number" in value:
                        return "ic"
                    if "inj" in value:
                        return "ic"


                    if "project" in value: # if it is a sampleID file that wasn't converted to a .csv
                        return "excel_sampleID"
                    i = i + 1
                return "ic" # (by default)

            elif filePath.endswith(".csv"):

                if filePath.endswith("fp.csv"):
                    return "scan.fp"
                if filePath.endswith("log.csv"):
                    return "ignore"

                # with open(filePath, "r+", encoding='utf-16-le') as csvFile:
                #     blankLine = csvFile.readline()
                #     if "sep" in blankLine:
                #         line1 = csvFile.readline()
                #         line1 = line1.split(",")
                #         if "unit" in line1[0].lower() and "id" in line1[0].lower():
                #             line2 = csvFile.readline()
                #             line2 = line2.split(",")
                #             if "user" in line2[0].lower() and "id" in line2[0].lower():
                #                 return "YSI"

                # open the file and look at the firs
                if "ysi" in filePath or "YSI" in filePath:
                    return "YSI"

                with open(filePath) as csvFile:

                    reader = csv.reader(csvFile, delimiter=",")
                    try:

                        firstRow = next(reader)
                        secondRow = next(reader)
                        thirdRow = next(reader)

                        if len(secondRow) == 0:
                            secondRow = thirdRow

                        if 'analog0' in secondRow and 'analog1' in secondRow and 'analog2' in secondRow and 'analog3' in secondRow:
                            return 'smartrock'
                        
                        if "Inj" in firstRow[0] and "Inj" in firstRow[1] and "Type" in firstRow[2]:
                            return "ic_new"
                        
                        if firstRow[0].endswith(".LOG"):
                            return "field_eureka"
                        elif "Operator" in firstRow[0]:
                            return "aqualog"
                        elif "project" in secondRow[0].lower():
                            if "device" in secondRow[1].lower():
                                if "date" in secondRow[2].lower():
                                    return "sampleID"
                        elif "Eureka" in secondRow[0]:
                            return "field_eureka"
                        elif "Plot Title" in firstRow[0] or "Serial Num" in firstRow[0]:
                            # parse which type of hobo
                            sRow = ",".join(secondRow)
                            sRow = sRow.lower()
                            if "intensity" in sRow:
                                return "light_hobo"
                            elif "cm" in sRow and ("range" in sRow or 'conductivity' in sRow):
                                return "conductivity_hobo"
                            elif "do" in sRow and "mg" in sRow and "conc" in sRow:
                                return "dissolved_oxygen_hobo"
                            else:
                                return "field_hobo.csv"
                        elif "stamp" in firstRow[0]:
                            if filePath.endswith("_log.csv"):
                                return "unrecognized"
                            else:
                                return "scan.par"
                        elif "sep" in firstRow[0]:
                            return "elementar"
                        elif "Eureka" in thirdRow[0]:
                            return "field_eureka"
                        elif "Chem #" in str(firstRow[0]):
                            return 'masterScan'
                        else:
                            return "unrecognized"
                    except:
                        try:

                            df = pd.read_csv(filePath, header=None)

                            firstRow = df.iloc[0].tolist()
                            secondRow = df.iloc[1].tolist()
                            thirdRow = df.iloc[2].tolist()

                            if len(secondRow) == 0:
                                secondRow = thirdRow

                            if "Inj" in firstRow[0] and "Inj" in firstRow[1] and "Type" in firstRow[2]:
                                return "ic_new"

                            if firstRow[0].endswith(".LOG"):
                                return "field_eureka"
                            elif "Operator" in firstRow[0]:
                                return "aqualog"
                            elif "project" in secondRow[0].lower():
                                if "device" in secondRow[1].lower():
                                    if "date" in secondRow[2].lower():
                                        return "sampleID"
                            elif "Eureka" in secondRow[0]:
                                return "field_eureka"
                            elif "Plot Title" in firstRow[0] or "Serial Num" in firstRow[0]:
                                # parse which type of hobo
                                sRow = ",".join(secondRow)
                                sRow = sRow.lower()
                                if "intensity" in sRow:
                                    return "light_hobo"
                                elif "cm" in sRow and ("range" in sRow or 'conductivity' in sRow):
                                    return "conductivity_hobo"
                                elif "do" in sRow and "mg" in sRow and "conc" in sRow:
                                    return "dissolved_oxygen_hobo"
                                else:
                                    return "field_hobo.csv"
                            elif "stamp" in firstRow[0]:
                                if filePath.endswith("_log.csv"):
                                    return "unrecognized"
                                else:
                                    return "scan.par"
                            elif "sep" in firstRow[0]:
                                return "elementar"
                            elif "Eureka" in thirdRow[0]:
                                return "field_eureka"
                            elif "Chem #" in str(firstRow[0]):
                                return 'masterScan'
                            else:
                                return "unrecognized"
                        except:
                            # the file was empty (raised an error trying to access it)
                            print(traceback.format_exc())
                            return "no_data"

            elif filePath.endswith('.hobo'):
                return "field_hobo.hobo"
            elif filePath.endswith(".sql"):
                return "unrecognized"
            elif filePath.endswith('.fp'):
                return "scan.fp"
            elif filePath.endswith(".par"):
                return "scan.par"
            else:
                return "unrecognized"
        # except:

        # except:
        #     return "causedException"
