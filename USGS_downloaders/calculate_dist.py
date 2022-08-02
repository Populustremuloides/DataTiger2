import traceback
import numpy as np
import pandas as pd

def get_closest_usgs_sites(usgs):
    sites = pd.read_csv("/Users/ethanmcquhae/Box/AbbottLab/MasterSiteListDescription.csv")

    usgs_names = np.asarray(list(usgs["STAID"]))
    usgs = usgs[["LAT_GAGE","LNG_GAGE"]].dropna().to_numpy()

    #pulls sites 3 letter code
    sites = sites[~sites["id_code"].isna()].reset_index()
    siteNames = sites["id_code"]
    sites = sites[["y", "x"]].dropna().to_numpy()

    sites_dict = {}
    sites_dict_verbose = {}
    sites_list = []

    for i in range(sites.shape[0]):
        distances = []
        for row in usgs:
            dist = np.linalg.norm(sites[i] - row, 2)
            distances.append(dist)

        index = np.argmin(distances)

        indices_of_closest = np.asarray(distances).argsort()[:3]
        closest_sites = []

        for j in indices_of_closest:
            site = usgs_names[j]
            closest_sites.append(str(site))
            sites_list.append(site)

        closest_site = usgs_names[index]
        sites_dict[str(siteNames[i])] = str(closest_site)

        #the specified site and the next 2 closest 3LetterSites and their USGS site
        sites_dict_verbose[str(siteNames[i])] = closest_sites

    # sites = [int(x) for x in list(sites_dict.values())]
    sites = list(set(sites_list))
    return sites, sites_dict, sites_dict_verbose
