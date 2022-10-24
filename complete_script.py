#Import packages
import geopandas
import pandas as pd
import json
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

#Open shapefile
shapefile=geopandas.read_file('CA_Legislative_Districts.zip')

#Drop unneeded columns (optional)
shapefile=shapefile[['ID','AREA','DISTRICT','POPULATION','geometry']]
shapefile.head()

#Check shapefile projection
shapefile.crs

#Convert shapefile projection to WGS84 (unprojected)
shapefile_wgs84=shapefile.to_crs(4326)
shapefile_wgs84.crs

#Convert to pandas dataframe and convert and rename the geometry column
df = pd.DataFrame(shapefile_wgs84)
df = df.astype({'geometry':'str'}).rename(columns={'geometry':'POLYGON_COORDINATES'})
df.info()

#Connect to Snowflake
with open('demo308_auth.json') as f:
    data = json.load(f)
    username = data['username']
    password = data['password']
    account = data['account']
conn = snowflake.connector.connect(
       user=username,
       password=password,
       account=account)

#Set up context and create a table to load the data
conn.cursor().execute("USE WAREHOUSE ADHOC")
conn.cursor().execute("CREATE DATABASE IF NOT EXISTS GEO") conn.cursor().execute("USE DATABASE GEO")
conn.cursor().execute("CREATE OR REPLACE SCHEMA GEO_BOUNDARIES")
conn.cursor().execute("USE SCHEMA GEO_BOUNDARIES")
conn.cursor().execute("CREATE OR REPLACE TEMPORARY TABLE           \                                             
                       CA_LEGISLATIVE_DISTRICTS_TMP                \
                          (ID number, AREA float,                  \
                           DISTRICT varchar,                       \
                           POPULATION number,                      \
                           POLYGON_COORDINATES varchar)").fetchone()
#Load data into Snowflake
success, nchunks, nrows, _ = write_pandas(conn, df, 'CA_LEGISLATIVE_DISTRICTS_TMP')
print(success, nchunks, nrows)
                      
#Convert boundaries to geography data type and save as a permanent table
conn.cursor().execute("CREATE OR REPLACE TABLE                     \
                       CA_LEGISLATIVE_DISTRICTS                    \
                       AS SELECT                                   \
                                 ID,                               \
                                 AREA,                             \ 
                                 DISTRICT,                         \
                                 POPULATION,                       \
                                 TO_GEOGRAPHY(POLYGON_COORDINATES) \
                                  as DISTRICT_BOUNDARY. \
                      FROM CA_LEGISLATIVE_DISTRICTS_TMP").fetchone()
                      
#Sample query: Which legislative districts have the highest and the lowest number of cell towers per 1,000 population?
conn.cursor().execute( "SELECT                                     \
                            LD.DISTRICT,                           \  
                            LD.POPULATION,                         \ 
                            COUNT(*) TOWER_CNT,                    \                                              
                            ROUND((TOWER_CNT/POPULATION)*1000,0)   \
                                TOWERS_PER_1000                    \
                       from  GEO.TELCO.CELL_TOWERS_US CT,          \
                       GEO.GEO_BOUNDARIES.CA_LEGISLATIVE_DISTRICTS \  
                                                                LD \
                       Where                                       \
                          ST_CONTAINS(LD.DISTRICT_BOUNDARY,CT.GEO) \
                       GROUP BY 1,2 \
                       ORDER BY 4 DESC;").fetchall()
