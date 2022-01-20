from colorama import init
init(autoreset=True)
from colorama import Fore

from shapely import geometry
# from termcolor import colored

from sqlalchemy.dialects.postgresql import *
# from geoalchemy2 import Geometry

import geopandas as gpd

import math
from tqdm import tqdm
from dbfread import DBF

def shp_import(conn, shp_file, db_schema, table):

    print(95*f"{Fore.GREEN}=")
    print(f"{Fore.LIGHTCYAN_EX}[Shapefile import]\n")
    
    print(f"{Fore.GREEN} - Reading [{shp_file}] ...")
    
    shp_rows = len(DBF(f"{shp_file[:-3]}dbf"))
    print(f"{Fore.GREEN}   {shp_rows} geometry objects/rows.")

    conn.execute(f"DROP TABLE IF EXISTS {db_schema}.{table}")

    interval = math.ceil(shp_rows/5)
    for x in tqdm(range(0, shp_rows, interval), ncols=95, desc=f"{Fore.GREEN} - Importing SHP: "):
        openrd = gpd.read_file(shp_file, rows=slice(x, x+interval))
        openrd = openrd.explode()
        openrd = openrd.rename_geometry('geom')
        openrd.to_postgis(table, 
                          conn, 
                          schema=db_schema,
                          if_exists='append',
                          index=False
                          # index_label='gid',
                          # dtype={'geometry':Geometry(geometry_type='GEOMETRY', spatial_index=True, srid=27700)}
                      )
    print(f"{Fore.GREEN}   Done!")

def topo_pg(conn, db_schema, table):

  """ Creation of PostGIS network/road topology for a imported road network.  """
  
  print(95*f"{Fore.GREEN}=")
  print(f"{Fore.LIGHTCYAN_EX}[Builds a network topology based on the imported geometry information]\n")
  print(95*f"{Fore.GREEN}-")

  cursor = conn.cursor()
  
  print(f"{Fore.GREEN} - Preparing table columns ...")
  cursor.execute(f"DROP TABLE IF EXISTS {db_schema}.{table}_vertices_pgr")
  # cursor.execute(f"ALTER TABLE {db_schema}.{table} ALTER COLUMN geom TYPE geometry(LINESTRING, 27700) USING ST_GeometryN(geom, 1)")
  cursor.execute(f"ALTER TABLE {db_schema}.{table} ALTER COLUMN geom TYPE geometry(LINESTRING, 27700) USING ST_Force2D(geom)")
  cursor.execute(f"ALTER TABLE {db_schema}.{table} ADD COLUMN gid SERIAL PRIMARY KEY")
  cursor.execute(f"ALTER TABLE {db_schema}.{table} ADD COLUMN source integer")
  cursor.execute(f"ALTER TABLE {db_schema}.{table} ADD COLUMN target integer")
  conn.commit()
  print(f"{Fore.GREEN}   Done!")
  
  cursor.execute(f"SELECT MIN(gid), MAX(gid) FROM {db_schema}.{table}")
  min_id, max_id = cursor.fetchone()
  interval = math.ceil((max_id - min_id + 1)/10)
  
  for x in tqdm(range(min_id, max_id+1, interval), ncols=95, desc=f"{Fore.GREEN} - Creating vertices: "):
      cursor.execute(f"SELECT pgr_createTopology('{db_schema}.{table}', 0.000001, 'geom', 'gid', 'source', 'target', rows_where:='gid>={x} and gid<{x+interval}');")
      conn.commit()
  
  # print("")
  # for n in conn.notices[-4:-1]:
  #     print(n)

  print(f"{Fore.GREEN}   Done!")
  print("")

def topo_sa(conn2, db_schema, table):

  """ Creation of PostGIS network/road topology for a imported road network.  """
  
  print(95*f"{Fore.GREEN}=")
  print(f"{Fore.LIGHTCYAN_EX}[Creating a network topology for '{table}' table ]\n")
  print(95*f"{Fore.GREEN}-")

  print(f"{Fore.GREEN} - Preparing table columns ...")
  conn2.execute(f"DROP TABLE IF EXISTS {db_schema}.{table}_vertices_pgr")
  conn2.execute("COMMIT")
  # conn2.execute(f"ALTER TABLE {db_schema}.{table} ALTER COLUMN geom TYPE geometry(LINESTRING, 27700) USING ST_GeometryN(geom, 1)")
  conn2.execute(f"ALTER TABLE {db_schema}.{table} ALTER COLUMN geom TYPE geometry(LINESTRING, 27700) USING ST_Force2D(geom)")
  conn2.execute(f"ALTER TABLE {db_schema}.{table} ADD COLUMN gid SERIAL PRIMARY KEY")
  conn2.execute("COMMIT")
  conn2.execute(f"ALTER TABLE {db_schema}.{table} ADD COLUMN source integer")
  conn2.execute("COMMIT")
  conn2.execute(f"ALTER TABLE {db_schema}.{table} ADD COLUMN target integer")
  conn2.execute("COMMIT")
  print(f"{Fore.GREEN}   Done!")
  
  qry = conn2.execute(f"SELECT MIN(gid), MAX(gid) FROM {db_schema}.{table}").fetchone()
  min_id, max_id = qry
  interval = math.ceil((max_id - min_id + 1)/10)
  
  for x in tqdm(range(min_id, max_id+1, interval), ncols=95, desc=f"{Fore.GREEN} - Creating vertices: "):
      conn2.execute(f"SELECT pgr_createTopology('{db_schema}.{table}', 0.000001, 'geom', 'gid', 'source', 'target', rows_where:='gid>={x} and gid<{x+interval}');")
      conn2.execute("COMMIT")
  
  print(f"{Fore.LIGHTGREEN_EX}   Done!")
  print("")

def routelines_shp_export(conn, db_schema, dir_shp, shp_out):

  print(95*f"{Fore.GREEN}=")
  print(f"{Fore.LIGHTCYAN_EX}[Exporting final tables to {dir_shp}]\n")
  print("...")

  try:
    print(f"{Fore.GREEN} - Incomplete routelines export to shapefile")
    sql1 = f"SELECT * FROM {db_schema}.routelines_final rf WHERE EXISTS (SELECT bm_route_id FROM {db_schema}.routelines_incomplete ri WHERE ri.bm_route_id = rf.bm_routeid);"
    gdf = gpd.GeoDataFrame.from_postgis(sql1, conn, geom_col='geom', crs="EPSG:27700")
    gdf.to_file(f"{dir_shp}{shp_out}_incomplete.shp")
    print(f"{Fore.LIGHTGREEN_EX}   Done!\n")

  except Exception as error:
    print(f"{Fore.LIGHTGREEN_EX}   No incomplete routelines geometry to export!")

  finally:
    print(f"{Fore.GREEN} - Complete routelines export to shapefile")
    sql2 = f"SELECT * FROM {db_schema}.routelines_final rf WHERE NOT EXISTS (SELECT bm_route_id FROM {db_schema}.routelines_incomplete ri WHERE ri.bm_route_id = rf.bm_routeid);"
    gdf = gpd.GeoDataFrame.from_postgis(sql2, conn, geom_col='geom', crs="EPSG:27700")
    gdf.to_file(f"{dir_shp}{shp_out}.shp")
    print(f"{Fore.LIGHTGREEN_EX}   Done!")