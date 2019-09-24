#%% Load Libs
import geopandas
from pathlib import Path
import pandas as pd
import datetime

#%% Setup paths
INPUT_PATH = Path.cwd() / "input"

# Paths to field boundaries
ce_boundary_path = INPUT_PATH / "20170206_CafRoughBoundaries" / "CafCookEastArea.shp"
cw_boundary_path = INPUT_PATH / "FromIanLeslie_CafGeospatial" / "CE_CW_WGS1984" / "CookWestBoundariesWGS1984" / "CookWestBoundariesWGS1984.shp"

# Paths to georeference points
ce_gp_path = INPUT_PATH / "CookEast_GeoReferencePoints_2016_IL" / "All_CookEast.shp"
cw_gp_path = INPUT_PATH / "FromIanLeslie_CafGeospatial" / "CE_CW_WGS1984" / "CookWestGeoRefPointsWGS1984" / "CookWestGeoRefPoints_WGS1984.shp"

#%% Clean and standardize boundaries

ce_boundary = (geopandas.read_file(ce_boundary_path)
    .to_crs({"init": "epsg:4326"})
    .drop(
        ["Id", "Area", "Perimeter", "Acres", "Hectares"],
        axis = 1)
    .assign(Id = "CE"))

cw_boundary = (geopandas.read_file(cw_boundary_path)
    .drop(
        ["Id", "POLY_AREA", "AREA_GEO", "PERIMETER", "PERIM_GEO"], 
        axis = 1)
    .assign(Id = "CW"))

#%% Clean and standardize georeference points

ce_gp_utm_11n = geopandas.read_file(ce_gp_path)
ce_gp_utm_11n.crs = {"init": "epsg:26911"}

ce_gp = (ce_gp_utm_11n
    .to_crs({"init": "epsg:4326"})
    .drop(
        ["FID_1", "COLUMN", "ROW", "ROW2", "COL_ROW", "COL_ROW2", 
        "EASTING", "NORTHING", "STRIP", "FIELD", "CROP", "AREA", 
        "PERIMETER", "AREA_AC", "TARGET"],
        axis = 1))

cw_gp = (geopandas.read_file(cw_gp_path)
    .drop(
        ["POINT_X", "POINT_Y"],
        axis = 1))

#%% Create data dictionaries
data_dictionary_columns = ["FieldName", "Units", "Description", "DataType"]
gp_data_dictionary = pd.DataFrame(
    data = [
        ["Id", 
        "unitless", 
        "Identifier of the field, used sometimes to identify experimental boundary or management boundary", 
        "String"]
    ],
    columns = data_dictionary_columns
)
boundary_data_dictionary = pd.DataFrame(
    data = [
        ["ID2",
        "unitless",
        "Numeric value used to identify georeferenced points for long-term sample collection. Values are unique among both Cook fields (CE and CW). Use 'ID2' instead of 'Id' for historic reasons.",
        "Int"]
    ],
    columns = data_dictionary_columns
)

#%% Output files

date = datetime.datetime.now().strftime("%Y%m%d")

OUT_PATH = Path.cwd() / "output"
OUT_PATH.mkdir(parents = True, exist_ok = True)

ce_boundary.to_file(
    Path(OUT_PATH / "cookeast_boundary_{}.geojson".format(date)), 
    driver="GeoJSON")
cw_boundary.to_file(
    Path(OUT_PATH / "cookwest_boundary_{}.geojson".format(date)), 
    driver="GeoJSON")
ce_gp.to_file(
    Path(OUT_PATH / "cookeast_georeferencepoint_{}.geojson".format(date)), 
    driver="GeoJSON")
cw_gp.to_file(
    Path(OUT_PATH / "cookwest_georeferencepoint_{}.geojson".format(date)), 
    driver="GeoJSON")

boundary_data_dictionary.to_csv(
    Path(OUT_PATH / "boundary_data_dictionary_{}.csv".format(date)),
    index = False)
gp_data_dictionary.to_csv(
    Path(OUT_PATH / "georeferencepoint_data_dictionary_{}.csv".format(date)),
    index = False)
