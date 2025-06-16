# Load Libs
import geopandas
from pathlib import Path
import pandas as pd
import datetime
from matplotlib import pyplot as plt

from geopandas.tools import sjoin

def main():
    # Setup paths
    INPUT_PATH = Path.cwd() / "input"

    # Paths to field boundaries
    ce_boundary_path = INPUT_PATH / "20170206_CafRoughBoundaries" / "CafCookEastArea.shp"
    cw_boundary_path = INPUT_PATH / "FromIanLeslie_CafGeospatial" / "CE_CW_WGS1984" / "CookWestBoundariesWGS1984" / "CookWestBoundariesWGS1984.shp"

    # Paths to georeference points
    ce_gp_path = INPUT_PATH / "CookEast_GeoReferencePoints_2016_IL" / "All_CookEast.shp"
    cw_gp_path = INPUT_PATH / "FromIanLeslie_CafGeospatial" / "CE_CW_WGS1984" / "CookWestGeoRefPointsWGS1984" / "CookWestGeoRefPoints_WGS1984.shp"

    # Paths to treatment boundaries
    treatment_paths = {
        "ce_treatment_path_1999To2015": INPUT_PATH / "CookEastStrips" / "Field_Plan_Final.shp",
        "ce_treatment_path_2016_C01": INPUT_PATH / "CE_WGS1984_2016_OperationalFieldBoundaries" / "C01" / "C0117001.shp",
        "ce_treatment_path_2016_C02": INPUT_PATH / "CE_WGS1984_2016_OperationalFieldBoundaries" / "C02" / "C0217001.shp",
        "ce_treatment_path_2017": INPUT_PATH / "20250307_CookEastFertZonesFromDataStream" / "CE_SW_2zones2017rates" / "CE_SW_2zones2017rates.shp",
        "ce_treatment_path_2018": "",  # No fert zones for 2018, crop was garbs
        "ce_treatment_path_2019": INPUT_PATH / "20250307_CookEastFertZonesFromDataStream" / "CE_WW_2zones2019" / "CE_WW_2zones2019.shp",
        "ce_treatment_path_2020": INPUT_PATH / "20250307_CookEastFertZonesFromDataStream" / "CE_C01_Fert_2zones2020_WGS84" / "CE_C01_Fert_2zones2020_WGS84.shp",
        "ce_treatment_path_2021": INPUT_PATH / "20250307_CookEastFertZonesFromDataStream" / "WSU_C01_Fert_2zones2021_WGS84" / "WSU_C01_Fert_2zones2021_WGS84.shp",
        "ce_treatment_path_2022": INPUT_PATH / "20250307_CookEastFertZonesFromDataStream" / "WSU_C01_Fert_2zones2022_WGS84" / "WSU_C01_Fert_2zones2022_WGS84" / "WSU_C01_Fert_2zones2022_WGS84.shp",
        "ce_treatment_path_2023": "",  # No fert zones for 2023, crop was winter peas
        # Fert zones used in 2024 were the same as in 2022
        "ce_treatment_path_2024": INPUT_PATH / "20250307_CookEastFertZonesFromDataStream" / "WSU_C01_Fert_2zones2022_WGS84" / "WSU_C01_Fert_2zones2022_WGS84" / "WSU_C01_Fert_2zones2022_WGS84.shp"
    }

    # CE Gridpoints
    ce_gp_utm_11n = geopandas.read_file(ce_gp_path)
    ce_gp_utm_11n.crs = "EPSG:26911"

    ce_gp = (ce_gp_utm_11n
        .to_crs("EPSG:4326")
        .drop(
            ["FID_1", "COLUMN", "ROW", "ROW2", "COL_ROW", "COL_ROW2", 
            "EASTING", "NORTHING", "CROP", "AREA", 
            "PERIMETER", "AREA_AC", "TARGET"],
            axis = 1))

    # Load and clean inputs
    df_all = pd.DataFrame()

    for treatment_path in treatment_paths.keys():

        if treatment_path == "ce_treatment_path_1999To2015":
            df = process_1999To2015(treatment_paths[treatment_path], ce_gp)
            
        elif treatment_path == "ce_treatment_path_2016_C01":
            df = process_2016_C01(treatment_paths[treatment_path], ce_gp)

        elif treatment_path == "ce_treatment_path_2016_C02":
            df = process_2016_C02(treatment_paths[treatment_path], ce_gp)
            
        elif treatment_path == "ce_treatment_path_2017":
            df = process_2017(treatment_paths[treatment_path], ce_gp)

        elif treatment_path == "ce_treatment_path_2018":
            df = process_2018(treatment_paths[treatment_path], ce_gp)
            
        elif treatment_path == "ce_treatment_path_2019":
            df = process_2019(treatment_paths[treatment_path], ce_gp)
            
        elif treatment_path == "ce_treatment_path_2020":
            df = process_2020(treatment_paths[treatment_path], ce_gp)
            
        elif treatment_path == "ce_treatment_path_2021":
            df = process_2021(treatment_paths[treatment_path], ce_gp)
            
        elif treatment_path == "ce_treatment_path_2022":
            df = process_2022(treatment_paths[treatment_path], ce_gp)

        elif treatment_path == "ce_treatment_path_2023":
            df = process_2023(treatment_paths[treatment_path], ce_gp)

        elif treatment_path == "ce_treatment_path_2024":
            df = process_2024(treatment_paths[treatment_path], ce_gp)
            
        if ("ce_treatment_path_2016" not in treatment_path) and (df.shape[0] != 369):
            raise Exception("Incorrect number of rows")
        
        if df_all.empty:
            df_all = df
        else:
            df_all = pd.concat([df_all, df], ignore_index=True)

    # Assign points in CE (1998 to current)
    df_ce_all = process_cookeast_1998_to_current(ce_boundary_path, ce_gp, 2024)
    if df_all.empty:
            df_all = df
    else:
        df_all = pd.concat([df_all, df_ce_all], ignore_index=True)


    # Create data dictionaries
    data_dictionary_columns = ["FieldName", "Units", "Description", "DataType"]

    georef_treatment_data_dictionary = pd.DataFrame(
        data = [
            ["ID2",
            "unitless",
            "Numeric value used to identify georeferenced points for long-term sample collection. Values are unique among both Cook fields (CE and CW). Use 'ID2' instead of 'Id' for historic reasons.",
            "Int"],
            ["PlotId",
            "unitless",
            "String designation used to identify the plot, a rough designation indicating common management practices (loosly an experimental unit), that the georeference point was located within for the given timespan between start and end year",
            "String"],
            ["TreatmentId",
            "unitless",
            "String designation used to identify the treatment that the georeference point was located within for the given timespan between start and end year. TreatmentId differs from PlotId between 1999-2015 because some treatments in Field C were split between two strips due to smaller area of the strips relative to those in Field A and Field B",
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

    # Output files

    date = datetime.datetime.now().strftime("%Y%m%d")

    OUT_PATH = Path.cwd() / "output"
    OUT_PATH.mkdir(parents = True, exist_ok = True)

    pd.DataFrame(df_all).sort_values(by = ["ID2", "StartYear"]).to_csv(
        Path(OUT_PATH / "georeferencepoint_treatments_cookeast_1999-2024_{}.csv".format(date)),
        index = False)

    georef_treatment_data_dictionary.to_csv(
        Path(OUT_PATH / "georeferencepoint_treatments_cookeast_1999-2024_Dictionary_{}.csv".format(date)),
        index = False)
    
def process_1999To2015(treatment_path, grid_points):
    # CE strips
    ce_tx_1999To2015_utm_11n = geopandas.read_file(treatment_path)
    ce_tx_1999To2015_utm_11n.crs = "EPSG:26911"

    ce_tx_1999To2015 = (ce_tx_1999To2015_utm_11n
        .to_crs("4326")
        .drop(
            ["Crop", "Area", "Perimeter", "Area_ac", "Ind_Field"],
            axis = 1))
    #ce_tx_1999To2016.plot()
    pointInPolys = sjoin(grid_points, ce_tx_1999To2015, how="left")

    # Check if original Strip and Field id's were assigned correctly
    if((pointInPolys["STRIP"] == pointInPolys["Strip"]).any() != True):
        raise Exception("Strips not equal") 
    if((pointInPolys["FIELD"] == pointInPolys["Field"]).any() != True):
        raise Exception("Fields not equal")

    # Clean up
    ce_1999To2015 = (pointInPolys
        .assign(PlotId = pointInPolys["Field"].astype(str) + pointInPolys["Strip"].astype(str))
        .assign(TreatmentId = pointInPolys["Field"].astype(str) + pointInPolys["Strip"].astype(str))
        .assign(StartYear = 1999)
        .assign(EndYear = 2015)
        .drop(["geometry", "STRIP", "FIELD", "index_right", "Strip", "Field"], axis = 1))

    # Reassign TreatmentIds at Field C; some treatments in Field C were split between two strips due to smaller area of the strips relative to those in Field A and Field B
    ce_1999To2015.loc[(ce_1999To2015["TreatmentId"] == "C8"), "TreatmentId"] = "C5"
    ce_1999To2015.loc[(ce_1999To2015["TreatmentId"] == "C7"), "TreatmentId"] = "C6"

    return pd.DataFrame(ce_1999To2015)

def process_2016_C01(treatment_path, grid_points):
    # CE strips    
    ce_tx_2016 = geopandas.read_file(treatment_path)
    ce_tx_2016.crs = "EPSG:4326"
    
    pointInPolys = sjoin(grid_points, ce_tx_2016, how="left")

    ce_2016 = (pointInPolys[pointInPolys["Description"] == "0"]
            .assign(PlotId = "C01_2016")
            .assign(TreatmentId = "C01_2016")
            .assign(StartYear = 2016)
            .assign(EndYear = 2016)
            .drop(["geometry", "STRIP", "FIELD", "index_right", "Description"], axis = 1))

    return pd.DataFrame(ce_2016)

def process_2016_C02(treatment_path, grid_points):
    # CE strips
    ce_tx_2016 = geopandas.read_file(treatment_path)
    ce_tx_2016.crs = "EPSG:4326"

    pointInPolys = sjoin(grid_points, ce_tx_2016, how="left")

    ce_2016 = (pointInPolys[pointInPolys["Description"] == "0"]
            .assign(PlotId = "C02_2016")
            .assign(TreatmentId = "C02_2016")
            .assign(StartYear = 2016)
            .assign(EndYear = 2016)
            .drop(["geometry", "STRIP", "FIELD", "index_right", "Description"], axis = 1))
    
    return pd.DataFrame(ce_2016)

def process_2017(treatment_path, grid_points):
    # CE strips
    #ce_tx_2017 = geopandas.read_file(treatment_path)
    #ce_tx_2017.crs = {"init": "epsg:4326"}

    ce_tx_2017_utm_11n = geopandas.read_file(treatment_path)
    ce_tx_2017_utm_11n.crs = "EPSG:26911"

    ce_tx_2017 = (ce_tx_2017_utm_11n
        .to_crs("EPSG:4326"))

    pointInPolys = sjoin(grid_points, ce_tx_2017, how="left")

    #ce_2017["PlotId"] = ce_2017["Zone"].apply(lambda x: "HighFertRate" if x == 1 else "LowFertRate")
    
    ce_2017 = (pointInPolys
            .assign(PlotId = pointInPolys["Zone"].apply(lambda x: "CE_HighFertZone_2017" if x == 1 else "CE_LowFertZone_2017"))
            .assign(TreatmentId = "ASP")
            .assign(StartYear = 2017)
            .assign(EndYear = 2017)
            .drop(["geometry", "STRIP", "FIELD", "index_right", "Id", "Id_1", "Zone", "Rate"], axis = 1))
    
    return pd.DataFrame(ce_2017)

def process_2018(treatment_path, grid_points):   
    # Crop was garbs, so no fert zones this year

    ce_2018 = (grid_points
            .assign(PlotId = "CE")
            .assign(TreatmentId = "ASP")
            .assign(StartYear = 2018)
            .assign(EndYear = 2018)
            .drop(["geometry", "STRIP", "FIELD"], axis = 1))
    
    return pd.DataFrame(ce_2018)

def process_2019(treatment_path, grid_points):
    ce_tx_2019_utm_11n = geopandas.read_file(treatment_path)
    ce_tx_2019_utm_11n.crs = "EPSG:26911"

    ce_tx_2019 = (ce_tx_2019_utm_11n
        .to_crs("EPSG:4326"))

    pointInPolys = sjoin(grid_points, ce_tx_2019, how="left")
    
    ce_2019 = (pointInPolys
            .assign(PlotId = pointInPolys["Zone"].apply(lambda x: "CE_HighFertZone_2019" if x == 1 else "CE_LowFertZone_2019"))
            .assign(TreatmentId = "ASP")
            .assign(StartYear = 2019)
            .assign(EndYear = 2019)
            .drop(["geometry", "STRIP", "FIELD", "index_right", "Id", "Id_1", "Zone", "Rate"], axis = 1))
    
    return pd.DataFrame(ce_2019)

def process_2020(treatment_path, grid_points):
    # CE strips    
    ce_tx_2020 = geopandas.read_file(treatment_path)
    ce_tx_2020.crs = "EPSG:4326"
    
    pointInPolys = sjoin(grid_points, ce_tx_2020, how="left")

    # Meaning of fert zone values were swapped in 2020
    ce_2020 = (pointInPolys
            .assign(PlotId = pointInPolys["Id"].apply(lambda x: "CE_LowFertZone_2020" if x == 1 else "CE_HighFertZone_2020"))
            .assign(TreatmentId = "ASP")
            .assign(StartYear = 2020)
            .assign(EndYear = 2020)
            .drop(["geometry", "STRIP", "FIELD", "index_right", "Id"], axis = 1))

    return pd.DataFrame(ce_2020)

def process_2021(treatment_path, grid_points):
    # CE strips    
    ce_tx_2021 = geopandas.read_file(treatment_path)
    ce_tx_2021.crs = "EPSG:4326"
    
    pointInPolys = sjoin(grid_points, ce_tx_2021, how="left")

    ce_2021 = (pointInPolys
            .assign(PlotId = pointInPolys["Zone"].apply(lambda x: "CE_HighFertZone_2021" if x == 1 else "CE_LowFertZone_2021"))
            .assign(TreatmentId = "ASP")
            .assign(StartYear = 2021)
            .assign(EndYear = 2021)
            .drop(["geometry", "STRIP", "FIELD", "index_right", "OBJECTID", "Id", "Id_1", "Zone", "Rate"], axis = 1))
    
    return pd.DataFrame(ce_2021)

def process_2022(treatment_path, grid_points):
    # CE strips    
    ce_tx_2022 = geopandas.read_file(treatment_path)
    ce_tx_2022.crs = "EPSG:4326"
    
    pointInPolys = sjoin(grid_points, ce_tx_2022, how="left")

    ce_2022 = (pointInPolys
            .assign(PlotId = pointInPolys["Zone"].apply(lambda x: "CE_HighFertZone_2022" if x == 1 else "CE_LowFertZone_2022"))
            .assign(TreatmentId = "ASP")
            .assign(StartYear = 2022)
            .assign(EndYear = 2022)
            .drop(["geometry", "STRIP", "FIELD", "index_right", "OBJECTID", "Id", "Id_1", "Zone", "Rate"], axis = 1))
    
    return pd.DataFrame(ce_2022)

def process_2023(treatment_path, grid_points):   
    # Crop was winter peas, so no fert zones this year

    ce_2023 = (grid_points
            .assign(PlotId = "CE")
            .assign(TreatmentId = "ASP")
            .assign(StartYear = 2023)
            .assign(EndYear = 2023)
            .drop(["geometry", "STRIP", "FIELD"], axis = 1))
    
    return pd.DataFrame(ce_2023)

def process_2024(treatment_path, grid_points):
    # CE strips    
    ce_tx_2024 = geopandas.read_file(treatment_path)
    ce_tx_2024.crs = "EPSG:4326"
    
    pointInPolys = sjoin(grid_points, ce_tx_2024, how="left")

    ce_2024 = (pointInPolys
            # NOTE: Using fert zones from 2022 because they were used again in 2024
            .assign(PlotId = pointInPolys["Zone"].apply(lambda x: "CE_HighFertZone_2024" if x == 1 else "CE_LowFertZone_2024"))
            .assign(TreatmentId = "ASP")
            .assign(StartYear = 2024)
            .assign(EndYear = 2024)
            .drop(["geometry", "STRIP", "FIELD", "index_right", "OBJECTID", "Id", "Id_1", "Zone", "Rate"], axis = 1))
    
    return pd.DataFrame(ce_2024)

def process_cookeast_1998_to_current(treatment_path, grid_points, end_year):
    ce_boundary_utm = geopandas.read_file(treatment_path)
    ce_boundary_utm.crs = '26911'
    ce_boundary = ce_boundary_utm.to_crs('4326')

    pointsInPolys = sjoin(grid_points, ce_boundary, how="left")

    ce = (pointsInPolys
        .assign(PlotId = "CE")
        .assign(TreatmentId = "ASP")
        .assign(StartYear = 1998)
        .assign(EndYear = end_year)
        .drop(["geometry", "STRIP", "FIELD", "index_right", "Id", "Area", "Perimeter", "Acres", "Hectares"], axis = 1))
    
    return pd.DataFrame(ce)

if __name__ == "__main__":
    main()