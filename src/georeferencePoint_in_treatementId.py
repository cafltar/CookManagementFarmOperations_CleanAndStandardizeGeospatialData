#%% Load Libs
import geopandas
from pathlib import Path
import pandas as pd
import datetime

from geopandas.tools import sjoin

#%% Setup paths
INPUT_PATH = Path.cwd() / "input"

# Paths to field boundaries
ce_boundary_path = INPUT_PATH / "20170206_CafRoughBoundaries" / "CafCookEastArea.shp"
cw_boundary_path = INPUT_PATH / "FromIanLeslie_CafGeospatial" / "CE_CW_WGS1984" / "CookWestBoundariesWGS1984" / "CookWestBoundariesWGS1984.shp"

# Paths to georeference points
ce_gp_path = INPUT_PATH / "CookEast_GeoReferencePoints_2016_IL" / "All_CookEast.shp"
cw_gp_path = INPUT_PATH / "FromIanLeslie_CafGeospatial" / "CE_CW_WGS1984" / "CookWestGeoRefPointsWGS1984" / "CookWestGeoRefPoints_WGS1984.shp"

# Paths to treatment boundaries
ce_treatment_path_1999To2016 = INPUT_PATH / "CookEastStrips" / "Field_Plan_Final.shp"
ce_treatment_path_2016 = INPUT_PATH / "CE_WGS1984_2016_OperationalFieldBoundaries" / "C01" / "C0117001.shp"
ce_treatment_path_2017 = INPUT_PATH / "20200408_CookEastFertZones" / "CE_SW_2zones2017rates" / "CE_SW_2zones2017rates.shp"

#%% Load and clean inputs

# CE Gridpoints
ce_gp_utm_11n = geopandas.read_file(ce_gp_path)
ce_gp_utm_11n.crs = {"init": "epsg:26911"}

ce_gp = (ce_gp_utm_11n
    .to_crs({"init": "epsg:4326"})
    .drop(
        ["FID_1", "COLUMN", "ROW", "ROW2", "COL_ROW", "COL_ROW2", 
        "EASTING", "NORTHING", "CROP", "AREA", 
        "PERIMETER", "AREA_AC", "TARGET"],
        axis = 1))
#%%
# CE strips
ce_tx_1999To2016_utm_11n = geopandas.read_file(ce_treatment_path_1999To2016)
ce_tx_1999To2016_utm_11n.crs = {"init": "epsg:26911"}

ce_tx_1999To2016 = (ce_tx_1999To2016_utm_11n
    .to_crs({"init": "epsg:4326"})
    .drop(
        ["Crop", "Area", "Perimeter", "Area_ac", "Ind_Field"],
        axis = 1))

#ce_tx_1999To2016.plot()
# %%
pointInPolys = sjoin(ce_gp, ce_tx_1999To2016, how="left")

# Check if original Strip and Field id's were assigned correctly
if((pointInPolys["STRIP"] == pointInPolys["Strip"]).any() != True):
    raise Exception("Strips not equal") 
if((pointInPolys["FIELD"] == pointInPolys["Field"]).any() != True):
    raise Exception("Fields not equal") 

# Clean up
ce_1999To2016 = (pointInPolys
    .assign(TreatmentId = pointInPolys["Field"].astype(str) + pointInPolys["Strip"].astype(str))
    .assign(StartYear = 1999)
    .assign(EndYear = 2015)
    .drop(["geometry", "STRIP", "FIELD", "index_right", "Strip", "Field"], axis = 1))


# %%
points_in_treatment = pd.DataFrame(ce_1999To2016).to_csv("foo.csv", index = False)

#%% Create data dictionaries
data_dictionary_columns = ["FieldName", "Units", "Description", "DataType"]

georef_treatment_data_dictionary = pd.DataFrame(
    data = [
        ["ID2",
        "unitless",
        "Numeric value used to identify georeferenced points for long-term sample collection. Values are unique among both Cook fields (CE and CW). Use 'ID2' instead of 'Id' for historic reasons.",
        "Int"],
        ["TreatmentId",
        "unitless",
        "String designation used to identify the treatment that the georeference point was located within for the given timespan between start and end year",
        "String"],
        ["StartYear",
        "unitless",
        "The harvest year that the treatment designation was first assigned, inclusive",
        "String"],
        ["EndYear",
        "unitless",
        "The harvest year that the treatment designation ended, inclusive",
        "String"]
    ],
    columns = data_dictionary_columns
)

#%% Output files

date = datetime.datetime.now().strftime("%Y%m%d")

OUT_PATH = Path.cwd() / "output"
OUT_PATH.mkdir(parents = True, exist_ok = True)

pd.DataFrame(ce_1999To2016).to_csv(
    Path(OUT_PATH / "georeferencepoint_treatments_cookeast_1999-2016_{}.csv".format(date)),
    index = False)

georef_treatment_data_dictionary.to_csv(
    Path(OUT_PATH / "georeferencepoint_treatments_cookeast_1999-2016_Dictionary_{}.csv".format(date)),
    index = False)
