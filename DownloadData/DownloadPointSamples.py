import webbrowser
import json
import sqlite3
import tornado.ioloop
from tornado.web import *
import traceback
import pandas as pd
import datetime
import os

def downloadAllPointSamples(outputPath, ROOT, conn):
    today = datetime.date.today()
    today_string = today.strftime("%y%m%d")
    export_file_name = os.path.join(outputPath, f"all_point_samples_{today_string}.xlsx")
    
    # Get site information from Master Site List
    site_df = pd.read_csv(os.path.join(ROOT, "MasterSiteListDescription.csv"))
    id_code_to_site_id = site_df[~site_df['id_code'].isna()]
    id_code_dict = {}

    for i,r in id_code_to_site_id.iterrows():
        id_code_dict[r['id_code']] = r['site_id']

    site_df = site_df[['x', 'y', 'site_id', 'name_of_water_body']]

    # pull q data from database. We're handling it separately from the rest because it doesn't have a sort_chem key
    # we also format the dates to allow for merging based on site_id + date_sampled later
    q_query = "SELECT q.site_id, q.date_sampled, q.time_sampled AS q_time_sampled, q.discharge FROM q_reads q"
    q = pd.read_sql(q_query, conn)
    q['date_sampled'] = q['date_sampled'].apply(lambda x: x if " " not in x else x.split(" ")[0])
    q['date_sampled'] = q['date_sampled'].apply(
        lambda x: "-".join(list(map(lambda y: y.zfill(2), x.split(" ")[0].split("-")))) if not pd.isna(x) else x)

    # queries shows us which tables to pull data from (the following should include all "grab sample" type data that we collect in person (ie on Synoptic)
    try:
        queries = [
            "SELECT icp.sort_chem, icp.icp_batch_id, icp.aluminum, icp.arsenic, icp.boron, icp.barium, icp.calcium, icp.cadmium, icp.cobalt, icp.chromium, icp.copper, icp.iron, icp.potassium, icp.magnesium, icp.manganese, icp.molybdenum, icp.sodium, icp.nickel, icp.phosphorus, icp.lead, icp.sulfur, icp.selenium, icp.silicon, icp.strontium, icp.titanium, icp.vanadium, icp.zinc FROM icp_reads_1 icp",
            "SELECT s.sort_chem, s.site_id, s.project_id, s.date_sampled, s.time_sampled, s.ph, s.orp, s.o2_percent, s.o2_mg, s.conductance, s.temperature, s.pressure, s.calibrated, s.q_salt_grams, s.q_time, s.q_date, s.notes, s.conditions, s.samplers, s.volume_filtered, s.aqualog_yes, s.doc_isotopes_yes, s.elementar_yes, s.scan_yes, s.ic_yes, s.icp_yes, s.lachat_yes, s.no3_isotopes_yes, s.srp_yes, s.water_isotopes_yes, s.ignore_yes, s.datetime_uploaded, s.file_path, s.device, s.chlorophyl_ugl, s.chlorophyl_rfu, s.pc_ug, s.tss_yes FROM sort_chems s",
            "SELECT tss.sort_chem, tss.tss_batch_id, tss.date_collected, tss.user_name, tss.sample_volume, tss.initial_filter_weight, tss.final_filter_weight, tss.tss, tss.drying_notes FROM tss_reads tss",
            "SELECT srp.sort_chem, srp.new_srp_batch_id, srp.no3 AS no3_from_srp, srp.nh4, srp.srp FROM new_srp_reads srp",
            "SELECT lachat.sort_chem, lachat.lachat_batch_id, lachat.no3_ppm AS no3_from_lachat, lachat.no4_ppm FROM lachat_reads lachat",
            "SELECT scan.sort_chem, scan.scan_master_batch_id, scan.datetime_run, scan.turbidity, scan.toc, scan.no3, scan.doc, scan.scan_master_batch_id FROM scan_master_reads scan",
            "SELECT icc.sort_chem, icc.batch_id, icc.lithium AS lithium_ , icc.sodium AS sodium_ , icc.magnesium AS magnesium_ , icc.potassium AS potassium_ , icc.calcium AS calcium_ , icc.ammonium AS ammonium_ , icc.strontium AS strontium_  FROM ic_cation_view icc",
            "SELECT ica.sort_chem, ica.fluoride, ica.acetate, ica.formate, ica.chloride, ica.nitrite, ica.bromide, ica.nitrate, ica.sulfate, ica.phosphate FROM ic_anion_view ica",
            "SELECT aqualog.sort_chem, aqualog.operator, aqualog.aqualog_batch_id, aqualog.corrected_by, aqualog.date_corrected, aqualog.project_file, aqualog.sample_name, aqualog.corrected_name, aqualog.blank_name, aqualog.dilution_factor, aqualog.fi370, aqualog.fi310, aqualog.fi254, aqualog.abs254, aqualog.slp274_295, aqualog.slp350_400, aqualog.sr, aqualog.ese3 FROM aqualog_view aqualog",
            "SELECT * FROM elementar_average_view elementar"
            ]

        # create df from each sql query and add it to a list
        tables = []
        for query in queries:
            df = pd.read_sql(query, conn)
            tables.append(df)

        # initialize our df with the first in the list, then merge with other df's using sort_chem as a key
        df = tables[0]
        for t in tables[1:]:
            t = t.drop_duplicates()
            df = pd.merge(df, t, on=['sort_chem'], how='outer')

        # Filter out unneeded columns
        cols = df.columns.tolist()
        cols = [x for x in cols if"_yes" not in x and 'batch_id' not in x and 'file' not in x and not 'operator' in x and '_instrument_id' not in x and not x.startswith('q_')]
        rmv = ['comments', 'samplers', 'tss_batch_id', 'device', 'file_name', 'user_name', 'file_path', 'date_run','operator', 'calibrated', 'conditions', 'date_corrected', 'corrected_by', 'corrected_name','sample_name', 'project_file', 'blank_name', 'dilution_factor', 'method', 'time_run', 'hole']
        cols = [x for x in cols if x not in rmv]
        df = df[cols]

        # Drop duplicates
        df = df.sort_values('ph', ascending=False).drop_duplicates().sort_index().reset_index(drop=True)
        df = df.sort_values(by="sort_chem")

        # There are lots of duplicate sortchem rows that contain different combinations of values, this squashes all non-null values into one row
        rows = []
        seen_rows = []
        for i, r in df.iterrows():
            if r['sort_chem'] not in seen_rows:
                rows.append(r.to_dict())
                seen_rows.append(r['sort_chem'])
            else:
                for k, v in r.items():
                    curr_val = rows[-1][k]
                    curr_sc = r['sort_chem']
                    if pd.isnull(curr_val):
                        if not pd.isnull(v):
                            if curr_sc != rows[-1]['sort_chem']:
                                raise ValueError('Sort chems didn\'t equal each other, this should never happen though.')
                            
                            rows[-1][k] = v

                    # always use the longer date when given the choice
                    elif k == 'date_sampled' and v is not None and len(curr_val) < len(v):
                        rows[-1][k] = v
                        
        df = pd.DataFrame(rows)

        # The following line pads date_sampled with requisite zeros (ie, 2017-7-6 becomes 2017-07-06)
        df['date_sampled'] = df['date_sampled'].apply(
            lambda x: "-".join(list(map(lambda y: y.zfill(2), x.split(" ")[0].split("-")))) if not pd.isna(
                x) else x)

        # Now we add the q (discharge) data from before, then replace id_code's with site_id's
        df = df.merge(q, on=['date_sampled', 'site_id'], how="outer")
        df['site_id'] = df['site_id'].replace(id_code_dict)

        # Here we separate rows into separate df's so we can sort by date and later combine them together again
        date_nan = df[df['date_sampled'].isna()].reset_index()
        df = df[~df['date_sampled'].isna()]
        has_hyphen = df[df['date_sampled'].str.contains("-")].reset_index()
        no_hyphen = df[~df['date_sampled'].str.contains("-")].reset_index()

        # has_hyphen includes all rows with dates we are able to parse. The following lines clean the dates so that they can be converted into datetime objects
        has_hyphen['date_sampled'] = has_hyphen['date_sampled'].apply(lambda x: x if len(x.split("-")[0]) < 3 else "-".join([x.split("-")[0][2:], x.split("-")[1], x.split("-")[2]]))
        has_hyphen['time_sampled'] = has_hyphen['time_sampled'].where(~has_hyphen['time_sampled'].isna(), "00:00")
        has_hyphen['time_sampled'] = has_hyphen['time_sampled'].where(has_hyphen['time_sampled'].str.contains("^\d{1,2}:\d{2}$"), "00:00")
        has_hyphen['date_sampled'] = has_hyphen['date_sampled'].str.replace(r"^(\d{2})-([^10]\d{1})-(\d{2})", r"\2-\3-\1")
        has_hyphen['date_sampled'] = has_hyphen['date_sampled'].where(has_hyphen['date_sampled'].str.contains("^\d{4}-\d{2}-\d{2}"), "20" + has_hyphen['date_sampled'])

        has_hyphen['date'] = has_hyphen['date_sampled'] + " " + has_hyphen['time_sampled']
        no_hyphen['date'] = no_hyphen['date_sampled']
        date_nan['date'] = date_nan['date_sampled']

        try:
            has_hyphen['date'] = pd.to_datetime(has_hyphen.date, format='%Y-%m-%d %H:%M')
        except:
            print(traceback.format_exc())
            has_hyphen['date'] = pd.to_datetime(has_hyphen.date_sampled, format='%Y-%m-%d')


        # Add site information
        has_hyphen = has_hyphen.merge(site_df, on='site_id', how='left')
        no_hyphen = no_hyphen.merge(site_df, on='site_id', how='left')
        date_nan = date_nan.merge(site_df, on='site_id', how='left')

        has_hyphen = has_hyphen.sort_values(by="date").reset_index()
        final = pd.concat([has_hyphen, no_hyphen, date_nan])

        # rename columns
        final = final.rename(columns={'strontium_': 'strontium ', 'ammonium_': 'ammonium ', 'calcium_': 'calcium ',
                                      'potassium_': 'potassium ', 'magnesium_': 'magnesium ', 'sodium_': 'sodium ',
                                      'lithium_': 'lithium ', 'o2_percent': 'DO%', 'o2_mg': 'DO(mg/L)',
                                      'temperature': 'Temp(Celsius)', 'pressure': 'Pressure',
                                      'conductance': 'SPConductivity(uS/cm)', 'ph': 'pH', 'orp': 'ORP',
                                      'chlorophyl_ugl': 'Chl(ug/L)', 'chlorophyl_rfu': 'Chl(RFU)',
                                      'pc_ug': 'PC(ug/L)', 'no3_from_srp': 'ppm NO3', 'no3': 'NO3', 'toc': "TOC",
                                      'doc': 'DOC', 'turbidity': 'Turbidity', 'nh4': 'ppm NH4', 'srp': 'ppm SRP',
                                      'tss': 'tss_g/mL', 'fi370': 'FI(FI370)', 'fi310': 'BIX(FI310)',
                                      'fi254': "HIX(FI254)", 'abs254': 'Abs254', 'sr': 'SR', 'ese3': 'ES_E3'})

        # The following contains information to sort the columns
        # Disregard color information, that was there to automate styling the excel and it didn't work out :)
        sections = {'Site and sampling info': [['sort_chem', 'date', 'site_id', 'name_of_water_body', 'x', 'y', 'notes'],'red'], 'Handheld sensor measurements': [['Temp(Celsius)', 'Pressure', 'DO%', 'DO(mg/L)', 'SPConductivity(uS/cm)', 'pH', 'ORP', 'Chl(ug/L)','Chl(RFU)', 'PC(ug/L)'], 'orange'], 'Nutrients': [['ppm NO3', 'ppm NH4', 'ppm SRP'], 'yellow'], 'Particulates': [['tss_g/mL'], 'green'],'Absorbance-device (S-CAN)': [['Turbidity', 'NO3', 'TOC', 'DOC'], 'teal'], 'ICP (mg/L)': [['aluminum', 'arsenic', 'boron', 'barium', 'calcium', 'cadmium', 'cobalt', 'chromium', 'copper','iron', 'potassium', 'magnesium', 'manganese', 'molybdenum', 'sodium', 'nickel', 'phosphorus','lead', 'sulfur', 'selenium', 'silicon', 'strontium', 'titanium', 'vanadium', 'zinc'], 'blue'],'Elemental analyzer (mg/L)': [['AVG(tic_mg_per_liter)', 'AVG(tc_mg_per_liter)', 'AVG(npoc_mg_per_liter)', 'AVG(tnb_mg_per_liter)','AVG(tic_area)', 'AVG(tc_area)', 'AVG(npoc_area)', 'AVG(tnb_area)'], 'purple'],'IC data (ions in uM)': [['lithium ', 'sodium ', 'ammonium ', 'potassium ', 'magnesium ', 'calcium ', 'strontium ','fluoride', 'acetate', 'formate', 'chloride', 'nitrite', 'bromide', 'nitrate', 'sulfate','phosphate'], 'violet'], 'Fluorescence spectroscopy (EEMS)': [['FI(FI370)', 'BIX(FI310)', 'HIX(FI254)', 'Abs254', 'slp274_295', 'slp350_400', 'SR', 'ES_E3'],'brown'], "lachat": [["no3_from_lachat", "no4_ppm"], 'white'],'Q': [['discharge', 'date_collected', 'datetime_run', 'project_id_x', 'project_id_y'], 'black']}
        ordered = []
        # Header cols are the organizational headers we'll add at the very end
        header_cols = []

        for k, v in sections.items():
            for column in v[0]:
                # Add blank strings between header_cols
                if f"{k} ->" not in header_cols:
                    header_cols.append(f"{k} ->")
                else:
                    header_cols.append("")

                # Organize columns
                ordered.append(column)

        # filter columns
        remainder = set(final.columns.tolist()) - set(ordered)
        final = final[ordered]

        # Save excel sheet
        final.to_excel(export_file_name, index=False)

        # Read excel sheet back in to add the organizational headers
        final = pd.read_excel(export_file_name, header=None)

        # Fill in with blanks so headers both have the same length
        h = final.columns.tolist()
        for x in range(len(h) - len(header_cols)):
            header_cols.append('')

        # This is an annoying way to add the headers together but it works :)
        header_cols = {h[i]: [header_cols[i]] for i in range(len(h))}
        df = pd.concat([pd.Series(v, name=k) for k, v in header_cols.items()], axis=1)
        final = pd.concat([df, final])

        # Save final export!
        final.to_excel(export_file_name, index=False, header=False)

        return "successfully downloaded point sample report to " + outputPath
    except:
        print(traceback.format_exc())
        return "failed to download point sample report to " + outputPath + ": " + traceback.format_exc()
    
def downloadUVUPointSamples(outputPath, ROOT, conn):
    today = datetime.date.today()
    today_string = today.strftime("%y%m%d")
    export_file_name = os.path.join(outputPath, f"uvu_export_{today_string}.xlsx")

    # Get site information from Master Site List
    site_df = pd.read_csv(os.path.join(ROOT, "MasterSiteListDescription.csv"))

    # Give uvu_site_name precedence in naming of water body since it's their data ¯\_(ツ)_/¯
    site_df['name_of_water_body'] = site_df['name_of_water_body'].where(site_df['uvu_site_name'].isna(), site_df['uvu_site_name'])
    id_code_to_site_id = site_df[~site_df['id_code'].isna()]
    id_code_dict = {}

    for i, r in id_code_to_site_id.iterrows():
        id_code_dict[r['id_code']] = r['site_id']

    # pull q data from database. We're handling it separately from the rest because it doesn't have a sort_chem key
    # we also format the dates to allow for merging based on site_id + date_sampled later
    q_query = "SELECT q.site_id, q.date_sampled, q.time_sampled AS q_time_sampled, q.discharge FROM q_reads q"
    q = pd.read_sql(q_query, conn)
    q['date_sampled'] = q['date_sampled'].apply(lambda x: x if " " not in x else x.split(" ")[0])
    q['date_sampled'] = q['date_sampled'].apply(
        lambda x: "-".join(list(map(lambda y: y.zfill(2), x.split(" ")[0].split("-")))) if not pd.isna(x) else x)

    # queries shows us which tables to pull data from (the following should include all "grab sample" type data that we collect in person (ie on Synoptic)
    try:
        queries = [
            "SELECT icp.sort_chem, icp.icp_batch_id, icp.aluminum, icp.arsenic, icp.boron, icp.barium, icp.calcium, icp.cadmium, icp.cobalt, icp.chromium, icp.copper, icp.iron, icp.potassium, icp.magnesium, icp.manganese, icp.molybdenum, icp.sodium, icp.nickel, icp.phosphorus, icp.lead, icp.sulfur, icp.selenium, icp.silicon, icp.strontium, icp.titanium, icp.vanadium, icp.zinc FROM icp_reads_1 icp",
            "SELECT s.sort_chem, s.site_id, s.project_id, s.date_sampled, s.time_sampled, s.ph, s.orp, s.o2_percent, s.o2_mg, s.conductance, s.temperature, s.pressure, s.calibrated, s.q_salt_grams, s.q_time, s.q_date, s.notes as notes_from_sort_chem, s.conditions, s.samplers, s.volume_filtered, s.aqualog_yes, s.doc_isotopes_yes, s.elementar_yes, s.scan_yes, s.ic_yes, s.icp_yes, s.lachat_yes, s.no3_isotopes_yes, s.srp_yes, s.water_isotopes_yes, s.ignore_yes, s.datetime_uploaded, s.file_path, s.device, s.chlorophyl_ugl, s.chlorophyl_rfu, s.pc_ug, s.tss_yes FROM sort_chems s",
            "SELECT tss.sort_chem, tss.tss_batch_id, tss.date_collected, tss.user_name, tss.sample_volume, tss.initial_filter_weight, tss.final_filter_weight, tss.tss, tss.drying_notes FROM tss_reads tss",
            "SELECT srp.sort_chem, srp.new_srp_batch_id, srp.no3 AS no3_from_srp, srp.nh4, srp.srp FROM new_srp_reads srp",
            "SELECT lachat.sort_chem, lachat.lachat_batch_id, lachat.no3_ppm AS no3_from_lachat, lachat.no4_ppm FROM lachat_reads lachat",
            "SELECT scan.sort_chem, scan.scan_master_batch_id, scan.datetime_run, scan.turbidity, scan.toc, scan.no3, scan.doc, scan.scan_master_batch_id FROM scan_master_reads scan",
            "SELECT icc.sort_chem, icc.batch_id, icc.lithium AS lithium_ , icc.sodium AS sodium_ , icc.magnesium AS magnesium_ , icc.potassium AS potassium_ , icc.calcium AS calcium_ , icc.ammonium AS ammonium_ , icc.strontium AS strontium_  FROM ic_cation_view icc",
            "SELECT ica.sort_chem, ica.fluoride, ica.acetate, ica.formate, ica.chloride, ica.nitrite, ica.bromide, ica.nitrate, ica.sulfate, ica.phosphate FROM ic_anion_view ica",
            "SELECT aqualog.sort_chem, aqualog.operator, aqualog.aqualog_batch_id, aqualog.corrected_by, aqualog.date_corrected, aqualog.project_file, aqualog.sample_name, aqualog.corrected_name, aqualog.blank_name, aqualog.dilution_factor, aqualog.fi370, aqualog.fi310, aqualog.fi254, aqualog.abs254, aqualog.slp274_295, aqualog.slp350_400, aqualog.sr, aqualog.ese3 FROM aqualog_view aqualog",
            "SELECT * FROM elementar_average_view elementar"
        ]

        # create df from each sql query and add it to a list
        tables = []
        for query in queries:
            df = pd.read_sql(query, conn)
            tables.append(df)

        # initialize our df with the first in the list, then merge with other df's using sort_chem as a key
        df = tables[0]
        for t in tables[1:]:
            t = t.drop_duplicates()
            df = pd.merge(df, t, on=['sort_chem'], how='outer')

        # Filter out unneeded columns
        cols = df.columns.tolist()
        cols = [x for x in cols if
                "_yes" not in x and 'batch_id' not in x and 'file' not in x and not 'operator' in x and '_instrument_id' not in x and not x.startswith(
                    'q_')]
        rmv = ['comments', 'samplers', 'tss_batch_id', 'device', 'file_name', 'user_name', 'file_path', 'date_run',
               'operator', 'calibrated', 'conditions', 'date_corrected', 'corrected_by', 'corrected_name',
               'sample_name', 'project_file', 'blank_name', 'dilution_factor', 'method', 'time_run', 'hole']
        cols = [x for x in cols if x not in rmv]
        df = df[cols]

        # Drop duplicates
        df = df.sort_values('ph', ascending=False).drop_duplicates().sort_index().reset_index(drop=True)
        df = df.sort_values(by="sort_chem")

        # There are lots of duplicate sortchem rows that contain different combinations of values, this squashes all non-null values into one row
        rows = []
        seen_rows = []
        for i, r in df.iterrows():
            if r['sort_chem'] not in seen_rows:
                rows.append(r.to_dict())
                seen_rows.append(r['sort_chem'])
            else:
                for k, v in r.items():
                    curr_val = rows[-1][k]
                    curr_sc = r['sort_chem']
                    if pd.isnull(curr_val):
                        if not pd.isnull(v):
                            if curr_sc != rows[-1]['sort_chem']:
                                raise ValueError(
                                    'Sort chems didn\'t equal each other, this should never happen though.')

                            rows[-1][k] = v

                    # always use the longer date when given the choice
                    elif k == 'date_sampled' and v is not None and len(curr_val) < len(v):
                        rows[-1][k] = v

        df = pd.DataFrame(rows)

        # The following line pads date_sampled with requisite zeros (ie, 2017-7-6 becomes 2017-07-06)
        df['date_sampled'] = df['date_sampled'].apply(
            lambda x: "-".join(list(map(lambda y: y.zfill(2), x.split(" ")[0].split("-")))) if not pd.isna(
                x) else x)

        # Now we add the q (discharge) data from before, then replace id_code's with site_id's
        df = df.merge(q, on=['date_sampled', 'site_id'], how="outer")
        df['site_id'] = df['site_id'].replace(id_code_dict)

        # Here we separate rows into separate df's so we can sort by date and later combine them together again
        date_nan = df[df['date_sampled'].isna()].reset_index()
        df = df[~df['date_sampled'].isna()]
        has_hyphen = df[df['date_sampled'].str.contains("-")].reset_index()
        no_hyphen = df[~df['date_sampled'].str.contains("-")].reset_index()

        # has_hyphen includes all rows with dates we are able to parse. The following lines clean the dates so that they can be converted into datetime objects
        has_hyphen['date_sampled'] = has_hyphen['date_sampled'].apply(
            lambda x: x if len(x.split("-")[0]) < 3 else "-".join(
                [x.split("-")[0][2:], x.split("-")[1], x.split("-")[2]]))
        has_hyphen['time_sampled'] = has_hyphen['time_sampled'].where(~has_hyphen['time_sampled'].isna(), "00:00")
        has_hyphen['time_sampled'] = has_hyphen['time_sampled'].where(
            has_hyphen['time_sampled'].str.contains("^\d{1,2}:\d{2}$"), "00:00")
        has_hyphen['date_sampled'] = has_hyphen['date_sampled'].str.replace(r"^(\d{2})-([^10]\d{1})-(\d{2})",
                                                                            r"\2-\3-\1", regex=True)
        has_hyphen['date_sampled'] = has_hyphen['date_sampled'].where(
            has_hyphen['date_sampled'].str.contains("^\d{4}-\d{2}-\d{2}"), "20" + has_hyphen['date_sampled'])

        has_hyphen['date'] = has_hyphen['date_sampled'] + " " + has_hyphen['time_sampled']
        no_hyphen['date'] = no_hyphen['date_sampled']
        date_nan['date'] = date_nan['date_sampled']

        try:
            has_hyphen['date'] = pd.to_datetime(has_hyphen.date, format='%Y-%m-%d %H:%M')
        except:
            print(traceback.format_exc())
            has_hyphen['date'] = pd.to_datetime(has_hyphen.date_sampled, format='%Y-%m-%d')

        # Add site information
        has_hyphen = has_hyphen.merge(site_df, on='site_id', how='left')
        no_hyphen = no_hyphen.merge(site_df, on='site_id', how='left')
        date_nan = date_nan.merge(site_df, on='site_id', how='left')

        has_hyphen = has_hyphen.sort_values(by="date").reset_index()
        final = pd.concat([has_hyphen, no_hyphen, date_nan])

        # FILTER BY UVU SITES

        # Get all sites that UVU is interested in from masterfieldbook
        df = pd.read_csv(os.path.join(ROOT, "Data/Fieldsheets/MasterFieldbook210831.csv"), skiprows=1)

        uvu = ['uvu', 'UVU', 'Uvu']
        uvu = df.loc[df['Event type (Synoptic/UVU/Baseline)'].str.lower().isin(uvu)]
        sites = list(set(uvu.Site.values.tolist()))

        sdf = pd.read_csv(os.path.join(ROOT, "MasterSiteListDescription.csv"))
        extra_uvu = sdf["site_id"][sdf["project_uvu"] == 1].values.tolist()
        sites = list(set(sites + extra_uvu))

        # Clean UVU sites (some of them are missing periods etc)
        extra = []
        for site in sites:
            if len(site.split(" ")) > 2:
                extra.append(".".join(site.split(" ")[:-1]))
            elif "UL" in site:
                extra.append(f"UL.{site.split('UL')[1]}")
                extra.append(site)
            else:
                site = site.replace("NBS ", "NBS.")
                extra.append(site)

        uvu = ['uvu', 'UVU', 'Uvu']
        uvu = site_df.loc[site_df['project_megafire'].str.lower().isin(uvu)]
        sites = sites + list(set(uvu.site_id.values.tolist()))

        # Clean those sites too
        for site in sites:
            if "UL" in site:
                extra.append(f"UL.{site.split('UL')[1]}")
        sites = list(set(sites + extra))

        final = final[final['site_id'].isin(sites)]
        final = final.drop(['notes'], axis=1)

        # rename columns
        final = final.rename(columns={'strontium_': 'strontium ', 'ammonium_': 'ammonium ', 'calcium_': 'calcium ',
                                      'potassium_': 'potassium ', 'magnesium_': 'magnesium ', 'sodium_': 'sodium ',
                                      'lithium_': 'lithium ', 'o2_percent': 'DO%', 'o2_mg': 'DO(mg/L)',
                                      'temperature': 'Temp(Celsius)', 'pressure': 'Pressure',
                                      'conductance': 'SPConductivity(uS/cm)', 'ph': 'pH', 'orp': 'ORP',
                                      'chlorophyl_ugl': 'Chl(ug/L)', 'chlorophyl_rfu': 'Chl(RFU)',
                                      'pc_ug': 'PC(ug/L)', 'no3_from_srp': 'ppm NO3', 'no3': 'NO3', 'toc': "TOC",
                                      'doc': 'DOC', 'turbidity': 'Turbidity', 'nh4': 'ppm NH4', 'srp': 'ppm SRP',
                                      'tss': 'tss_g/mL', 'fi370': 'FI(FI370)', 'fi310': 'BIX(FI310)',
                                      'fi254': "HIX(FI254)", 'abs254': 'Abs254', 'sr': 'SR', 'ese3': 'ES_E3', 'notes_from_sort_chem': 'notes'})

        # The following contains information to sort the columns
        # Disregard color information, that was there to automate styling the excel and it didn't work out :)
        sections = {
            'Site and sampling info': [['sort_chem', 'date', 'site_id', 'name_of_water_body', 'x', 'y', 'notes'],
                                       'red'], 'Handheld sensor measurements': [
                ['Temp(Celsius)', 'Pressure', 'DO%', 'DO(mg/L)', 'SPConductivity(uS/cm)', 'pH', 'ORP', 'Chl(ug/L)',
                 'Chl(RFU)', 'PC(ug/L)'], 'orange'], 'Nutrients': [['ppm NO3', 'ppm NH4', 'ppm SRP'], 'yellow'],
            'Particulates': [['tss_g/mL'], 'green'],
            'Absorbance-device (S-CAN)': [['Turbidity', 'NO3', 'TOC', 'DOC'], 'teal'], 'ICP (mg/L)': [
                ['aluminum', 'arsenic', 'boron', 'barium', 'calcium', 'cadmium', 'cobalt', 'chromium', 'copper', 'iron',
                 'potassium', 'magnesium', 'manganese', 'molybdenum', 'sodium', 'nickel', 'phosphorus', 'lead',
                 'sulfur', 'selenium', 'silicon', 'strontium', 'titanium', 'vanadium', 'zinc'], 'blue'],
            'Elemental analyzer (mg/L)': [
                ['AVG(tic_mg_per_liter)', 'AVG(tc_mg_per_liter)', 'AVG(npoc_mg_per_liter)', 'AVG(tnb_mg_per_liter)',
                 'AVG(tic_area)', 'AVG(tc_area)', 'AVG(npoc_area)', 'AVG(tnb_area)'], 'purple'],
            'IC data (ions in uM)': [
                ['lithium ', 'sodium ', 'ammonium ', 'potassium ', 'magnesium ', 'calcium ', 'strontium ', 'fluoride',
                 'acetate', 'formate', 'chloride', 'nitrite', 'bromide', 'nitrate', 'sulfate', 'phosphate'], 'violet'],
            'Fluorescence spectroscopy (EEMS)': [
                ['FI(FI370)', 'BIX(FI310)', 'HIX(FI254)', 'Abs254', 'slp274_295', 'slp350_400', 'SR', 'ES_E3'],
                'brown'], "lachat": [["no3_from_lachat", "no4_ppm"], 'white'],
            'Q': [['discharge'], 'black'],
            'Miscellaneous': [['date_collected', 'datetime_run', 'project_id_x', 'project_id_y'], 'black']}
        ordered = []
        # Header cols are the organizational headers we'll add at the very end
        header_cols = []

        for k, v in sections.items():
            for column in v[0]:
                # Add blank strings between header_cols
                if f"{k} ->" not in header_cols:
                    header_cols.append(f"{k} ->")
                else:
                    header_cols.append("")

                # Organize columns
                ordered.append(column)

        # filter columns
        remainder = set(final.columns.tolist()) - set(ordered)
        final = final[ordered]

        # Save excel sheet
        final.to_excel(export_file_name, index=False)

        # Read excel sheet back in to add the organizational headers
        final = pd.read_excel(export_file_name, header=None)

        # Fill in with blanks so headers both have the same length
        h = final.columns.tolist()
        for x in range(len(h) - len(header_cols)):
            header_cols.append('')

        # This is an annoying way to add the headers together but it works :)
        header_cols = {h[i]: [header_cols[i]] for i in range(len(h))}
        df = pd.concat([pd.Series(v, name=k) for k, v in header_cols.items()], axis=1)
        final = pd.concat([df, final])

        # Save final export!
        final.to_excel(export_file_name, index=False, header=False)

        return "successfully downloaded point sample report to " + outputPath
    except:
        print(traceback.format_exc())
        return "failed to download point sample report to " + outputPath + ": " + traceback.format_exc()

    # try:
    #     # Get all sites that UVU is interested in from masterfieldbook
    #     df = pd.read_csv(os.path.join(ROOT, "Data/Fieldsheets/MasterFieldbook210831.csv"), skiprows=1)
    #
    #     uvu = ['uvu', 'UVU', 'Uvu']
    #     uvu = df.loc[df['Event type (Synoptic/UVU/Baseline)'].str.lower().isin(uvu)]
    #     sites = list(set(uvu.Site.values.tolist()))
    #
    #     sdf = pd.read_csv(os.path.join(ROOT, "MasterSiteListDescription.csv"))
    #     extra_uvu = sdf["site_id"][sdf["project_uvu"] == 1].values.tolist()
    #     sites = list(set(sites + extra_uvu))
    #
    #     # Clean UVU sites (some of them are missing periods etc)
    #     extra = []
    #     for site in sites:
    #         if len(site.split(" ")) > 2:
    #             extra.append(".".join(site.split(" ")[:-1]))
    #         elif "UL" in site:
    #             extra.append(f"UL.{site.split('UL')[1]}")
    #             extra.append(site)
    #         else:
    #             site = site.replace("NBS ", "NBS.")
    #             extra.append(site)
    #
    #     # Get additional UVU sites from mastersitelist
    #     site_df = pd.read_csv(os.path.join(ROOT, "MasterSiteListDescription.csv"))
    #
    #     id_code_to_site_id = site_df[~site_df['id_code'].isna()]
    #     id_code_dict = {}
    #
    #     for i, r in id_code_to_site_id.iterrows():
    #         id_code_dict[r['id_code']] = r['site_id']
    #
    #     uvu = ['uvu', 'UVU', 'Uvu']
    #     uvu = site_df.loc[site_df['project_megafire'].str.lower().isin(uvu)]
    #     sites = sites + list(set(uvu.site_id.values.tolist()))
    #
    #     # Clean those sites too
    #     for site in sites:
    #         if "UL" in site:
    #             extra.append(f"UL.{site.split('UL')[1]}")
    #     sites = list(set(sites + extra))
    #
    #     # Give uvu_site_name precedence in naming of water body since it's their data ¯\_(ツ)_/¯
    #     site_df['name_of_water_body'] = site_df['name_of_water_body'].where(site_df['uvu_site_name'].isna(),
    #                                                                         site_df['uvu_site_name'])
    #
    #     # We're only interested in the following columns going forward so filter for those
    #     site_df = site_df[['x', 'y', 'site_id', 'name_of_water_body']]
    #
    #     try:
    #         # pull q data from database. We're handling it separately from the rest because it doesn't have a sort_chem key
    #         # we also format the dates to allow for merging based on site_id + date_sampled later
    #         q_query = "SELECT q.site_id, q.date_sampled, q.time_sampled AS q_time_sampled, q.discharge FROM q_reads q"
    #         q = pd.read_sql(q_query, conn)
    #         q['date_sampled'] = q['date_sampled'].apply(lambda x: x if " " not in x else x.split(" ")[0])
    #         q['date_sampled'] = q['date_sampled'].apply(
    #             lambda x: "-".join(list(map(lambda y: y.zfill(2), x.split(" ")[0].split("-")))) if not pd.isna(
    #                 x) else x)
    #
    #         # Pull data from database for all UVU sites
    #         vals = []
    #         for site in sites:
    #             query = """SELECT s.site_id, s.project_id, s.date_sampled, s.time_sampled, s.sort_chem, s.ph, s.orp, s.o2_percent, s.o2_mg, s.conductance, s.temperature, s.pressure, s.calibrated, s.q_salt_grams, s.q_time, s.q_date, s.notes, s.conditions, s.samplers, s.volume_filtered, s.aqualog_yes, s.doc_isotopes_yes, s.elementar_yes, s.scan_yes, s.ic_yes, s.icp_yes, s.lachat_yes, s.no3_isotopes_yes, s.srp_yes, s.water_isotopes_yes, s.ignore_yes, s.datetime_uploaded, s.file_path, s.device, s.chlorophyl_ugl, s.chlorophyl_rfu, s.pc_ug, s.tss_yes,
    #                          icp.icp_batch_id, icp.aluminum, icp.arsenic, icp.boron, icp.barium, icp.calcium, icp.cadmium, icp.cobalt, icp.chromium, icp.copper, icp.iron, icp.potassium, icp.magnesium, icp.manganese, icp.molybdenum, icp.sodium, icp.nickel, icp.phosphorus, icp.lead, icp.sulfur, icp.selenium, icp.silicon, icp.strontium, icp.titanium, icp.vanadium, icp.zinc,
    #                          tss.tss_batch_id, tss.date_collected, tss.user_name, tss.sample_volume, tss.initial_filter_weight, tss.final_filter_weight, tss.tss, tss.drying_notes,
    #                          srp.new_srp_batch_id, srp.no3 AS no3_from_srp, srp.nh4, srp.srp,
    #                          lachat.lachat_batch_id, lachat.no3_ppm AS no3_from_lachat, lachat.no4_ppm,
    #                          scan.scan_master_batch_id, scan.datetime_run, scan.turbidity, scan.toc, scan.no3, scan.doc, scan.scan_master_batch_id,
    #                          icc.batch_id, icc.lithium, icc.sodium, icc.magnesium, icc.potassium, icc.calcium, icc.ammonium, icc.strontium,
    #                          ica.fluoride, ica.acetate, ica.formate, ica.chloride, ica.nitrite, ica.bromide, ica.nitrate, ica.sulfate, ica.phosphate,
    #                          aqualog.operator, aqualog.aqualog_batch_id, aqualog.corrected_by, aqualog.date_corrected, aqualog.project_file, aqualog.sample_name, aqualog.corrected_name, aqualog.blank_name, aqualog.dilution_factor, aqualog.fi370, aqualog.fi310, aqualog.fi254, aqualog.abs254, aqualog.slp274_295, aqualog.slp350_400, aqualog.sr, aqualog.ese3
    #                         FROM sort_chems s
    #                         LEFT JOIN icp_reads_1 icp
    #                             ON icp.sort_chem = s.sort_chem
    #                         LEFT JOIN tss_reads tss
    #                             ON tss.sort_chem = s.sort_chem
    #                         LEFT JOIN new_srp_reads srp
    #                             ON srp.sort_chem = s.sort_chem
    #                         LEFT JOIN lachat_reads lachat
    #                             ON lachat.sort_chem = s.sort_chem
    #                         LEFT JOIN scan_master_reads scan
    #                             ON scan.sort_chem = s.sort_chem
    #                         LEFT JOIN ic_cation_view icc
    #                             ON icc.sort_chem = s.sort_chem
    #                         LEFT JOIN ic_anion_view ica
    #                             ON ica.sort_chem = s.sort_chem
    #                         LEFT JOIN aqualog_view aqualog
    #                             ON aqualog.sort_chem = s.sort_chem
    #                         LEFT JOIN elementar_average_view elementar
    #                             ON elementar.sort_chem = s.sort_chem
    #                         WHERE s.site_id =
    #                         """ + site
    #
    #             df = pd.read_sql(query, conn)
    #             vals.append(df)
    #
    #         # Build Dataframe from values
    #         df = vals[0]
    #         for v in vals[1:]:
    #             v = v.drop_duplicates()
    #             df = pd.merge(df, v, on=['sort_chem'], how='outer')
    #
    #         # We have to do it again for elementar data since it's a view :(
    #         vals = []
    #         for site in sites:
    #             sql = """SELECT *
    #                      FROM elementar_average_view elementar
    #                      INNER JOIN sort_chems s
    #                          ON elementar.sort_chem = s.sort_chem
    #                      WHERE s.site_id =
    #                      """ + site
    #             df = pd.read_sql(sql, conn)
    #             vals.append(df)
    #
    #         # Build Dataframe from values
    #         edf = vals[0]
    #         for v in vals[1:]:
    #             v = v.drop_duplicates
    #             edf = pd.merge(edf, v, on=['sort_chem'], how='outer')
    #
    #         edf = edf[['elementar_batch_id', 'hole', 'sort_chem', 'method', 'AVG(tic_area)', 'AVG(tc_area)', 'AVG(npoc_area)', 'AVG(tnb_area)', 'AVG(tic_mg_per_liter)', 'AVG(tc_mg_per_liter)', 'AVG(npoc_mg_per_liter)','AVG(tnb_mg_per_liter)', 'date_run', 'time_run']]
    #         edf = edf.drop_duplicates()
    #
    #         df = df.merge(edf, on='sort_chem', how='outer')
    #         df = df.drop_duplicates()
    #
    #         # Filter unneeded columns
    #         cols = df.columns.tolist()
    #         cols = [x for x in cols if"_yes" not in x and 'batch_id' not in x and '_instrument_id' not in x and not x.startswith('q_')]
    #         rmv = ['comments', 'samplers', 'tss_batch_id', 'datetime_uploaded', 'device', 'file_name', 'date_collected','user_name', 'file_path', 'project_id', 'datetime_run', 'date_run', 'operator', 'calibrated','conditions', 'date_corrected', 'corrected_by', 'corrected_name', 'sample_name', 'project_file','blank_name', 'dilution_factor', 'method', 'time_run', 'hole']
    #         cols = [x for x in cols if x not in rmv]
    #         df = df[cols]
    #
    #         # Drop duplicates
    #         df = df.sort_values('ph', ascending=False).drop_duplicates().sort_index().reset_index(drop=True)
    #
    #         # There are lots of duplicate sortchem rows that contain different combinations of values, this squashes all none null values into one row
    #         rows = []
    #         seen_rows = []
    #         for i, r in df.iterrows():
    #             if r['sort_chem'] not in seen_rows:
    #                 rows.append(r.to_dict())
    #                 seen_rows.append(r['sort_chem'])
    #             else:
    #                 for k, v in r.items():
    #                     curr_val = rows[-1][k]
    #                     curr_sc = r['sort_chem']
    #                     if pd.isnull(curr_val) and not pd.isnull(v):
    #                         if curr_sc != rows[-1]['sort_chem']:
    #                             raise ValueError(
    #                                 'Sort chems didn\tt equal each other, this should never happen though.')
    #                             print("ERR")
    #                             sys.exit(1)
    #                         rows[-1][k] = v
    #         df = pd.DataFrame(rows)
    #
    #         # Now we add the q (discharge) data from before, then replace id_code's with site_id's
    #         df = df.merge(q, on=['date_sampled', 'site_id'], how="left")
    #         df['site_id'] = df['site_id'].replace(id_code_dict)
    #
    #         # Here we separate rows into separate df's so we can sort by date and later combine them together again
    #         date_nan = df[df['date_sampled'].isna()].reset_index()
    #         df = df[~df['date_sampled'].isna()]
    #         has_hyphen = df[df['date_sampled'].str.contains("-")].reset_index()
    #         no_hyphen = df[~df['date_sampled'].str.contains("-")].reset_index()
    #
    #         # has_hyphen includes all rows with dates we are able to parse. The following lines clean the dates so that they can be converted into datetime objects
    #         has_hyphen['date_sampled'] = has_hyphen['date_sampled'].apply(lambda x: x if len(x.split("-")[0]) < 3 else "-".join([x.split("-")[0][2:], x.split("-")[1], x.split("-")[2]]))
    #         has_hyphen['time_sampled'] = has_hyphen['time_sampled'].where(~has_hyphen['time_sampled'].isna(), "00:00")
    #         has_hyphen['time_sampled'] = has_hyphen['time_sampled'].where(has_hyphen['time_sampled'].str.contains("^\d{1,2}:\d{2}$"), "00:00")
    #         has_hyphen['date_sampled'] = has_hyphen['date_sampled'].str.replace(r"^(\d{2})-([^10]\d{1})-(\d{2})", r"\2-\3-\1")
    #         has_hyphen['date_sampled'] = has_hyphen['date_sampled'].where(has_hyphen['date_sampled'].str.contains("^\d{4}-\d{2}-\d{2}"), "20" + has_hyphen['date_sampled'])
    #
    #         has_hyphen['date'] = has_hyphen['date_sampled'] + " " + has_hyphen['time_sampled']
    #         no_hyphen['date'] = no_hyphen['date_sampled']
    #         date_nan['date'] = date_nan['date_sampled']
    #
    #         try:
    #             has_hyphen['date'] = pd.to_datetime(has_hyphen.date, format='%Y-%m-%d %H:%M')
    #         except:
    #             print(traceback.format_exc())
    #             has_hyphen['date'] = pd.to_datetime(has_hyphen.date_sampled, format='%Y-%m-%d')
    #
    #         # Add site information
    #         has_hyphen = has_hyphen.merge(site_df, on='site_id', how='outer')
    #         no_hyphen = no_hyphen.merge(site_df, on='site_id', how='outer')
    #         date_nan = date_nan.merge(site_df, on='site_id', how='outer')
    #
    #         has_hyphen = has_hyphen.sort_values(by="date").reset_index()
    #         final = pd.concat([has_hyphen, no_hyphen, date_nan])
    #
    #         # # Parse dates
    #         # df['date_sampled'] = df['date_sampled'].apply(
    #         #     lambda x: "-".join(list(map(lambda y: y.zfill(2), x.split(" ")[0].split("-")))))
    #         # df['date_sampled'] = df['date_sampled'].apply(lambda x: "NA-NA-NA" if "-" not in x else x)
    #         # df['date_sampled'] = df['date_sampled'].apply(lambda x: x if len(x.split("-")[0]) < 3 else "-".join(
    #         #     [x.split("-")[0][2:], x.split("-")[1], x.split("-")[2]]))
    #         #
    #         # # These are rows that have error values in 'date_sampled', we'll save them for later
    #         # no_date = df.loc[df['date_sampled'].isin(["NA-NA-NA"])]
    #         # no_date['date'] = "error parsing date"
    #         # no_date['date_sampled'] = "error parsing date"
    #         #
    #         # # Remove error date rows from df and parse a little more
    #         # df = df.loc[~df['date_sampled'].isin(["NA-NA-NA"])]
    #         # df['time_sampled'] = df['time_sampled'].where(~df['time_sampled'].isna(), "00:00")
    #         # df['date'] = df['date_sampled'] + " " + df['time_sampled']
    #         # df['date'] = pd.to_datetime(df.date, format='%y-%m-%d %H:%M')
    #         # df = df.sort_values(by="sort_chem")
    #
    #         # Rename columns
    #         final = final.rename(columns={'o2_percent': 'DO%', 'o2_mg': 'DO(mg/L)', 'temperature': 'Temp(Celsius)','pressure': 'Pressure', 'conductance': 'SPConductivity(uS/cm)', 'ph': 'pH','orp': 'ORP', 'chlorophyl_ugl': 'Chl(ug/L)', 'chlorophyl_rfu': 'Chl(RFU)','pc_ug': 'PC(ug/L)', 'no3_from_srp': 'ppm NO3', 'no3': 'NO3', 'toc': "TOC",'doc': 'DOC', 'turbidity': 'Turbidity', 'nh4': 'ppm NH4', 'srp': 'ppm SRP','tss': 'tss_g/mL', 'fi370': 'FI(FI370)', 'fi310': 'BIX(FI310)','fi254': "HIX(FI254)", 'abs254': 'Abs254', 'sr': 'SR', 'ese3': 'ES_E3'})
    #
    #         # The following contains information to sort the columns
    #         # Disregard color information, that was there to automate styling the excel and it didn't work out :)
    #         sections = {'Site and sampling info': [['sort_chem', 'date', 'site_id', 'name_of_water_body', 'x', 'y', 'notes'],'red'], 'Handheld sensor measurements': [['Temp(Celsius)', 'Pressure', 'DO%', 'DO(mg/L)', 'SPConductivity(uS/cm)', 'pH', 'ORP', 'Chl(ug/L)','Chl(RFU)', 'PC(ug/L)'], 'orange'], 'Nutrients': [['ppm NO3', 'ppm NH4', 'ppm SRP'], 'yellow'],'Particulates': [['tss_g/mL'], 'green'],'Absorbance-device (S-CAN)': [['Turbidity', 'NO3', 'TOC', 'DOC'], 'teal'], 'ICP (mg/L)': [['aluminum', 'arsenic', 'boron', 'barium', 'calcium', 'cadmium', 'cobalt', 'chromium', 'copper','iron', 'potassium', 'magnesium', 'manganese', 'molybdenum', 'sodium', 'nickel', 'phosphorus','lead', 'sulfur', 'selenium', 'silicon', 'strontium', 'titanium', 'vanadium', 'zinc'], 'blue'],'Elemental analyzer (mg/L)': [['AVG(tic_mg_per_liter)', 'AVG(tc_mg_per_liter)', 'AVG(npoc_mg_per_liter)', 'AVG(tnb_mg_per_liter)','AVG(tic_area)', 'AVG(tc_area)', 'AVG(npoc_area)', 'AVG(tnb_area)'], 'purple'],'IC data (ions in uM)': [['lithium', 'sodium', 'ammonium', 'potassium', 'magnesium', 'calcium', 'strontium', 'fluoride','acetate', 'formate', 'chloride', 'nitrite', 'bromide', 'nitrate', 'sulfate', 'phosphate'],'violet'], 'Fluorescence spectroscopy (EEMS)': [['FI(FI370)', 'BIX(FI310)', 'HIX(FI254)', 'Abs254', 'slp274_295', 'slp350_400', 'SR', 'ES_E3'],'brown']}
    #         ordered = []
    #
    #         # Header cols are the organizational headers we'll add at the very end
    #         header_cols = []
    #
    #         for k, v in sections.items():
    #             for column in v[0]:
    #                 # Add blank strings between header_cols
    #                 if f"{k} ->" not in header_cols:
    #                     header_cols.append(f"{k} ->")
    #                 else:
    #                     header_cols.append("")
    #
    #                 # Organize columns
    #                 ordered.append(column)
    #
    #         # filter columns
    #         final = final[ordered]
    #
    #         # Save excel sheet
    #         final.to_excel(export_file_name, index=False)
    #
    #         # Read excel sheet back in to add the organizational headers
    #         final = pd.read_excel(export_file_name, header=None)
    #
    #         # Fill in with blanks so headers both have the same length
    #         h = final.columns.tolist()
    #         for x in range(len(h) - len(header_cols)):
    #             header_cols.append('')
    #
    #         # This is an annoying way to add the headers together but it works :)
    #         header_cols = {h[i]: [header_cols[i]] for i in range(len(h))}
    #         df = pd.concat([pd.Series(v, name=k) for k, v in header_cols.items()], axis=1)
    #         final = pd.concat([df, final])
    #
    #         # Save final export!
    #         final.to_excel(export_file_name, index=False, header=False)
    #
    #         result = 'success!'
    #     except:
    #         print(traceback.format_exc())
    #         result = 'removal failed'
    #
    #     self.close()
    #     self.render("uvu.html", sortchem_cols=sortchem_cols, result=result)
    # except:
    #     print(traceback.format_exc())



