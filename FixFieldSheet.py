

root = "C:\\Users\\BCBrown\\Box\\AbbottLab\\Data\\FieldSheets\\"
inFile = "MasterFieldbook200603.csv"
outFile = "CorrectedMasterFieldboko200604.csv"


def fixDate(date):
    date = date.replace("/","-")
    day, month, year = date.split("-")
    if len(year) == 2:
        year = int(year)
        year = year + 2000
        year = str(year)
    fixedDate = year + "-" + month + "-" + day
    return fixedDate

with open(root + outFile, "w+") as oFile:
    with open(root + inFile, "r+") as iFile:
        i = 0
        for line in iFile:
            if i > 1:
                line = line.replace("\n","")
                line = line.split(",")
                print(line)
                date = line[2]
                if date != "":
                    fixedDate = fixDate(date)
                    line[2] = fixedDate
                    outLine = ",".join(line)
                    outLine = outLine + "\n"
                    oFile.write(outLine)
                print(i)
            i = i + 1
