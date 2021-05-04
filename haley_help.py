import pandas as pd
import numpy as np


desktop = "C:\\Users\\BCBrown\\Desktop\\UVU\\"
box = ""

sitedf = pd.read_csv(desktop + "MasterSiteList.csv")
littlesitedf = sitedf[~sitedf["id_code"].isna()]
codes = littlesitedf["id_code"]
nbs = littlesitedf["id_site"]
codeDict = dict(zip(codes, nbs))
#print(sitedf)
#print(sitedf)
#quit()

# read in tss
tssdf = pd.read_csv(desktop + "USU_TSS_4-13-2021.csv")
#tssdf = pd.read_csv(desktop + "Total_Suspended_Solids20200125.csv") # the old one
tssSorts = set(list(tssdf["sort_chem"]))
print(tssdf)

# read in SCAN
scandf = pd.read_csv(desktop + "MasterScan_3.csv")
print(scandf)
#for col in scandf.columns:
#    print(col)
#quit()
scanSorts = set(list(scandf["sort_chem"]))

common = list(tssSorts.intersection(scanSorts))
print(len(common)) # a value of 69 is pretty good!

# read in SRP # FIXME: figure out what to do with multiple of these!
#srpdf = pd.read_csv(desktop + "srp.csv")
#srpSorts = set(list(srpdf["sort_chem"]))
#common = set(tssSorts.intersection(scanSorts))
#common = list(common.intersection(srpSorts))
#print(srpdf)
#quit()
print(len(common))

# read in ICP
icpdf = pd.read_csv(desktop + "ICP_data.csv") # fixme: redo this one

# read in elementar
tocdf = pd.read_csv(desktop + "elementar.csv")

# read in IC
iccationdf = pd.read_csv(desktop + "ic_cation.csv") # FIXME: generate this
icaniondf = pd.read_csv(desktop + "ic_anion.csv") # FIXME: generate this

# read in aqualog
aqualogdf = pd.read_csv(desktop + "aqualog.csv") # FIXME: generate this

# read in master field book
mfbdf = pd.read_csv(desktop + "MasterFieldbook200604_copy.csv")

no3df = pd.read_csv(desktop + "combined_no3_srp.csv")


# TODO: replace the siteIDs with the NBS number
print(mfbdf)


newSites = []
for index, row in mfbdf.iterrows():
    #print(row["site_id"])
    site = row["site_id"]
    if type(site) == type("string"):
        site = site.replace(" ","")
        if site in codeDict.keys():
            newSites.append(codeDict[site])
            #mfbdf.iloc[index]["site_id"] = codeDict[site]
            #print(row["site_id"])
            #print(mfbdf.iloc[index]["site_id"])
            #print(codeDict[site])
        elif "ul" in site.lower():
            site = site.replace(".","")
            newSites.append(site)
        else:
            newSites.append(site)

    else:
        newSites.append(site)
mfbdf["site_id"] = newSites
print(mfbdf["site_id"])

# remove NaN sort_chems
#srpdf = srpdf[~srpdf["sort_chem"].isna()]
tssdf = tssdf[~tssdf["sort_chem"].isna()]
mfbdf = mfbdf[~mfbdf["sort_chem"].isna()]
scandf = scandf[~scandf["sort_chem"].isna()]
icpdf = icpdf[~icpdf["sort_chem"].isna()]
sitedf = sitedf[~sitedf["site_id"].isna()]
tocdf = tocdf[~tocdf["sort_chem"].isna()]
iccationdf = iccationdf[~iccationdf["sort_chem"].isna()]
icaniondf = icaniondf[~icaniondf["sort_chem"].isna()]
aqualogdf = aqualogdf[~aqualogdf["sort_chem"].isna()]
no3df = no3df[~no3df["sort_chem"].isna()]

def replaceNA(df):
    for index, row in df.iterrows():
        mask = np.asarray(row.isna())
        #print(row)
        #print(mask)
        for i in range(len(mask)):
            if mask[i] == True:
                row[i] = "BDL"
                #print("none!")
                #print(df.iloc[index, i])
                df.iloc[index, i] = "BDL"
                #print(df.iloc[index, i])
                #input("press any key")

    return df

#tssdf = replaceNA(tssdf)
#scandf = replaceNA(scandf)
#icpdf = replaceNA(icpdf)
#tocdf = replaceNA(tocdf)
#iccationdf = replaceNA(iccationdf)
#icaniondf = replaceNA(icaniondf)
#aqualogdf = replaceNA(aqualogdf)
#no3df = replaceNA(aqualogdf)


# merge them all!
merged = mfbdf.merge(tssdf, on="sort_chem", how="left")
#merged = merged.merge(srpdf, on="sort_chem", how="left")
merged = merged.merge(scandf, on="sort_chem", how="left")
merged = merged.merge(icpdf, on="sort_chem", how="left")
merged = merged.merge(tocdf, on="sort_chem", how="left")
merged = merged.merge(iccationdf, on="sort_chem", how="left")
merged = merged.merge(icaniondf, on="sort_chem", how="left")
merged = merged.merge(aqualogdf, on="sort_chem", how="left")
merged = merged.merge(no3df, on="sort_chem", how="left")

#print(sitedf["site_id"])

for column in merged.columns:
    print(column)
#quit()
#    print(column == "site_id")
#for item in merged["site_id_x"]:
#    print(item)
#print(mfbset)
#print(mset)
#print(sset)
#print(mset.intersection(sset))
#print(mset.difference(sset))
#print(len(list(mset.intersection(sset))))
#print(len(list(mset.difference(sset))))


#print(mfbdf["site_id"])
merged["site_id"] = merged["site_id_x"]
merged = merged.merge(sitedf, on="site_id", how="left")
print(merged["project_uvu"])
#quit()
print("**************************************************")
# for index, row in merged.iterrows():
#     if type(row["project_uvu"]) == type(7.0):
#         print(row["project_uvu"])
#         print(row["sort_chem"])
#         print(row["site_id"])
#for site in merged["project_uvu"]:
#    print(site)
print("**************************************************")

mask = []
for index, row in merged.iterrows():
    if (row["project_uvu"] == 1) or ("WWTP" in row["site_id"]):
        # print(row["site_id"])
        mask.append(True)
    else:
        mask.append(False)

merged = merged[mask]
#quit()
#merged = merged[merged["project_uvu"] == 1]

def isNotZerosAndOnes(col, name):
    print(name)
    if "TSS_yes" == name:
        print("in here")
    isNotZerosAndOnes = True
    for item in col:
        if pd.isna(item):
            pass
        elif item == None:
            pass
        elif item == "0":
            pass
        elif item == 0:
            pass
        elif item == 0.0:
            pass
        elif item == "1":
            pass
        elif item == 1:
            pass
        elif item == 1.0:
            pass
        else:
            isNotZerosAndOnes = False
    return isNotZerosAndOnes


for column in merged.columns:
    if "unnamed" in column.lower():
        merged = merged.drop(column, axis=1)

merged = merged.groupby(by="sort_chem", as_index=False).first()
#merged = merged.groupby(by="sort_chem", as_index=False).bfill()
#merged = merged.drop_duplicates()
zerosAndOnes = []
for column in merged.columns:
    if isNotZerosAndOnes(merged[column], column):
        zerosAndOnes.append(column)
merged = merged.drop(zerosAndOnes, axis=1)
merged.to_csv(desktop + "uvu_data_comparison_comparison.csv", index=False)
#merged.to_csv(desktop + "uvu_data_BDL.csv", index=False)
print(merged)
quit()

quit()



byudf = pd.read_csv(desktop + "uvu_byu_sites.csv")
byudf = byudf.drop(byudf.columns[-1], axis=1)
byudf = byudf[byudf["type"] == "UVU"]
print(byudf)


sdf = pd.read_csv(desktop + "MasterFieldbook200604_copy.csv")

#sdf = sdf[sdf["sort_chem"] == "2020-1398"]
#print(sdf)
#keepers = fdf.merge(ldf, on="sort_chem")
keepers = sdf.merge(ldf, on="sort_chem", how="outer")
#for column in keepers.columns:
#    print(column)
#quit()
keepers = keepers.merge(byudf, on="site_id")
print(keepers)
keepers.to_csv(desktop + "UVU_scan_data.csv", index=False)
