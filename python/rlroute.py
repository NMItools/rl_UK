from colorama import init
init(autoreset=True)
from colorama import Fore
import math
from tqdm import tqdm
from setup import idx_no, buffer



def nodesuk(conn, db_schema, table, dir_cif, cif_file):
  """ Creation of nodesuk help table."""

  cursor = conn.cursor()

  print(95*f"{Fore.GREEN}=")
  print(f"{Fore.LIGHTCYAN_EX}[Creation of 'nodesuk' help table]\n")
    
  # ---------------------------------------------------------------------------------------------------------------------------------------------------------------------
  cursor.execute(f"DROP TABLE IF EXISTS {db_schema}.nodesuk;")
  conn.commit()
  cursor.execute('''CREATE TABLE '''+db_schema+'''.nodesuk(
                  id_pk serial NOT NULL,
                  stop_id character varying(20),
                  x integer,
                  y integer,
                  node_id integer,
                  edge_id integer,
                  fraction float,
                  edge_circ integer,
                  geom geometry(Point,27700),	
                  CONSTRAINT nodesuk_pk PRIMARY KEY (id_pk)) TABLESPACE pg_default;''')
  conn.commit()

  # ---------------------------------------------------------------------------------------------------------------------------------------------------------------------
  
  print(f"{Fore.GREEN} - Populating nodesuk table with unique BUS stops from CIF data ...\n")

  cursor.execute(f"INSERT INTO {db_schema}.nodesuk (stop_id, x, y) SELECT anodestopid, anodexcoord, anodeycoord FROM {db_schema}.abnodesuk")
  conn.commit()
  cursor.execute(f"INSERT INTO {db_schema}.nodesuk (stop_id, x, y) SELECT bnodestopid, bnodexcoord, bnodeycoord FROM {db_schema}.abnodesuk")
  conn.commit()

  cursor.execute(f"CREATE INDEX {idx_no}_geom_idx ON {db_schema}.nodesuk USING gist (geom) TABLESPACE pg_default")
  conn.commit()
  cursor.execute(f"CREATE INDEX {idx_no}_stop_idx ON {db_schema}.nodesuk USING btree (stop_id) TABLESPACE pg_default")
  conn.commit()

  cursor.execute('''
                  DELETE FROM '''+db_schema+'''.nodesuk 
                  WHERE id_pk IN (
                                  SELECT id_pk FROM  (SELECT id_pk, ROW_NUMBER() OVER( PARTITION BY stop_id ORDER BY id_pk) AS row_num 
                                  FROM '''+db_schema+'''.nodesuk) t 
                                  WHERE t.row_num > 1 
                                 )''')
  conn.commit()

  cursor.execute(f"ALTER TABLE {db_schema}.nodesuk DROP COLUMN id_pk CASCADE;")
  conn.commit()
  cursor.execute(f"ALTER TABLE {db_schema}.nodesuk ADD COLUMN id_pk SERIAL PRIMARY KEY")
  conn.commit()
  
  # -------------------------------------------------------------------------------------------------------------------
  cursor.execute(f"SELECT COUNT(*) FROM {db_schema}.nodesuk;")
  conn.commit()

  n_rows = cursor.fetchone()[0]
  interval = math.ceil(n_rows/10)
  # print(interval)
  # for x in range(0, n_rows, interval):
  #   print(x, x+interval)

  # -------------------------------------------------------------------------------------------------------------------
  print(f"{Fore.GREEN} - POINT geometry creation for BUS nodes with known X and Y coordinates ...")

  for x in tqdm(range(0, n_rows, interval), ncols=95, desc=f"{Fore.GREEN}   Creating POINT: "):
    cursor.execute(f"""UPDATE {db_schema}.nodesuk SET geom = ST_MakePoint(x, y) 
                       WHERE x != 0 AND y != 0 AND id_pk BETWEEN {x} AND {x+interval};""")
    conn.commit()

  cursor.execute(f"SELECT COUNT(*) FROM {db_schema}.nodesuk;")
  conn.commit()
  bus_ok = cursor.fetchone()[0]
  print(f"{Fore.LIGHTGREEN_EX}   Unique BUS stops with XY   : {bus_ok}")

  cursor.execute(f"SELECT COUNT(*) FROM {db_schema}.nodesuk WHERE geom is NULL;")
  conn.commit()
  no_xy = cursor.fetchone()[0]

  if no_xy != 0:
    print(f"{Fore.LIGHTRED_EX}   Unique BUS stops with no XY: {no_xy} - (this could cause incomplete routelines!)\n")
    sql = f"COPY (SELECT stop_id FROM {db_schema}.nodesuk WHERE geom is NULL ORDER BY stop_id) TO STDOUT WITH CSV DELIMITER ';'"
    with open(f"{dir_cif}{cif_file[:-4]}_unknown_bus_stops.csv", "w") as file:
        cursor.copy_expert(sql, file)
    print(f"{Fore.LIGHTRED_EX}   The list of unknown BUS stop ID's exported to [{dir_cif}{cif_file[:-4]}_unknown_bus_stops.csv]\n")
  else:
    print(f"{Fore.LIGHTGREEN_EX}   Unique BUS stops with no XY: {no_xy} - (ALL coordinates are known!)\n")

  # -------------------------------------------------------------------------------------------------------------------

  print(f"{Fore.GREEN} - Calculating nearest GRAPH NODE to a BUS stop ...")

  for x in tqdm(range(0, n_rows, interval), ncols=95, desc=f"{Fore.GREEN}   Nearest NODE  : "):
    cursor.execute(f"""UPDATE {db_schema}.nodesuk SET node_id = {db_schema}.get_nearest_node_1(stop_id) 
                       WHERE x != 0 AND y != 0 AND id_pk BETWEEN {x} AND {x+interval};""")
    conn.commit()

  # -------------------------------------------------------------------------------------------------------------------
  print(f"{Fore.GREEN} - Calculating nearest GRAPH EDGE to a BUS stop ...")

  for x in tqdm(range(0, n_rows, interval), ncols=95, desc=f"{Fore.GREEN}   Nearest EDGE  : "):
    cursor.execute(f"""UPDATE {db_schema}.nodesuk SET edge_id = {db_schema}.get_nearest_edge_1(stop_id)
                       WHERE x != 0 AND y != 0 AND id_pk BETWEEN {x} AND {x+interval};""")
    conn.commit()

  # -------------------------------------------------------------------------------------------------------------------
  print(f"{Fore.GREEN} - Update CIRC flag for a GRAPH EDGEs that has the same starting and ending node (circular)...")

  cursor.execute(f"""UPDATE {db_schema}.nodesuk 
                      SET edge_circ = 1
                      FROM (SELECT gid FROM {db_schema}.{table} WHERE source = target) rd
                      WHERE edge_id = rd.gid;""")
  conn.commit()

  # -------------------------------------------------------------------------------------------------------------------
  print(f"{Fore.GREEN} - Calculating distance from BUS stop to the nearest point on GRAPH NODE,\n"
         "   as a fraction of total 2d line length ...")

  for x in tqdm(range(0, n_rows, interval), ncols=95, desc=f"{Fore.GREEN}   BUS distance  : "):
    cursor.execute(f"""UPDATE {db_schema}.nodesuk SET fraction = {db_schema}.fraction_calc(edge_id, stop_id)
                       WHERE x != 0 AND y != 0 AND id_pk BETWEEN {x} AND {x+interval};""")
    conn.commit()

  print(f"{Fore.LIGHTGREEN_EX}Done!")

def abnodesuk_update(conn, db_schema, buffer):
  """ Updating abnodesuk table."""

  cursor = conn.cursor()

  print(95*f"{Fore.GREEN}-")
  print(f"{Fore.LIGHTCYAN_EX}[Updating 'abnodesuk' table]\n")

  cursor.execute(f"SELECT COUNT(*) FROM {db_schema}.abnodesuk;")
  ab_rows = cursor.fetchone()[0]

  cursor.execute(f"SELECT COUNT(*) FROM {db_schema}.abnodesuk WHERE (anodexcoord IS NULL OR bnodexcoord IS NULL) OR (anodexcoord = 0 OR bnodexcoord = 0);")
  no_xy = cursor.fetchone()[0]

  print(f"{Fore.LIGHTGREEN_EX}   AB routes with all known BUS stops   : {ab_rows-no_xy}")
  if no_xy != 0:
    print(f"{Fore.LIGHTRED_EX}   AB routes with unknown BUS stops     : {no_xy} - (this could cause incomplete routelines!)\n")
  else:
    print(f"{Fore.LIGHTGREEN_EX}   AB routes with unknown BUS stops     : {no_xy} - (ALL coordinates are known!)\n")

  interval = math.ceil(ab_rows/10)

  print(f"{Fore.GREEN} - Update node, edge, fraction, and circ flag values...")
  for x in tqdm(range(0, ab_rows, interval), ncols=95, desc=f"{Fore.GREEN} - Updating: "):
    cursor.execute(f"""UPDATE {db_schema}.abnodesuk 
                       SET a_fraction = nuk.fraction, a_edge = nuk.edge_id, a_node = nuk.node_id, a_edge_circ = nuk.edge_circ
                       FROM {db_schema}.nodesuk nuk 
                       WHERE anodestopid = nuk.stop_id
                       AND id BETWEEN {x} AND {x+interval};""")
    conn.commit()

    cursor.execute(f"""UPDATE {db_schema}.abnodesuk 
                       SET b_fraction = nuk.fraction, b_edge = nuk.edge_id, b_node = nuk.node_id, b_edge_circ = nuk.edge_circ 
                       FROM {db_schema}.nodesuk nuk 
                       WHERE bnodestopid = nuk.stop_id
                       AND id BETWEEN {x} AND {x+interval};""")
    conn.commit()

    #   update nearest graph node for B node when A and B nodes are on the same edge and share same geographicaly nearest graph node

  print(f"{Fore.GREEN} - Update nearest GRAPH NODE values for B stop (when both stops are on the same GRAPH EDGE)...")
  for x in tqdm(range(0, ab_rows, interval), ncols=95, desc=f"{Fore.GREEN} - Updating: "):
    cursor.execute(f"""UPDATE {db_schema}.abnodesuk
                      SET b_node = (SELECT * FROM {db_schema}.get_nearest_node_2(bnodestopid) OFFSET 1)
                      WHERE anodexcoord != 0 AND bnodexcoord != 0 
                      AND a_edge = b_edge 
                      AND a_node = b_node
                      AND a_edge_circ IS NULL
                      AND b_edge_circ IS NULL
                      AND id BETWEEN {x} AND {x+interval};""")
    conn.commit()

  print(f"{Fore.GREEN} - Update buffer size to {buffer} meters...")
  cursor.execute(f"UPDATE {db_schema}.abnodesuk SET buffer_m = {buffer} WHERE hops != 0")
  conn.commit()

  print(f"{Fore.LIGHTGREEN_EX}Done!")

def f_ab_routes_table(conn, db_schema, tablename):
  """ Creating ab_routes table """

  print(95*f"{Fore.GREEN}-")
  print(f"{Fore.LIGHTCYAN_EX}[Creating 'ab_routes' table]\n")
  
  cursor = conn.cursor()

  cursor.execute(f"SELECT {db_schema}.ab_routes_table('{db_schema}', '{tablename}');")
  conn.commit()
  print(f"{Fore.LIGHTGREEN_EX}Done!")
  
def f_ab_routes_calc(conn, db_schema):
  """ Creating shortest paths between A and B BUS stops"""

  cursor = conn.cursor()

  cursor.execute(f"SELECT COUNT(*) FROM {db_schema}.abnodesuk;")
  ab_rows = cursor.fetchone()[0]
  
  print(95*f"{Fore.GREEN}-")
  print(f"{Fore.LIGHTCYAN_EX}[Creating shortest paths for {ab_rows} pairs of A-B points]\n")
  
  interval = math.ceil(ab_rows/10)

  # ------------------------------------------------------------------------------------------------
  # PASS 0
  # ------------------------------------------------------------------------------------------------
  for x in tqdm(range(0, ab_rows, interval), ncols=95, desc=f"{Fore.GREEN} - Pass 0: "):
    cursor.execute(f"SELECT {db_schema}.ab_routes_calc_0('{x}', '{x+interval}', {buffer});")
    conn.commit()
  
  cursor.execute(f"SELECT COUNT(*) FROM {db_schema}.ab_routes;")
  pass0 = cursor.fetchone()[0]
  print(f"         : {pass0} paths")

  # ------------------------------------------------------------------------------------------------
  # PASS 1
  # ------------------------------------------------------------------------------------------------
  for x in tqdm(range(0, ab_rows, interval), ncols=95, desc=f"{Fore.GREEN} - Pass 1: "):
    cursor.execute(f"SELECT {db_schema}.ab_routes_calc_1('{x}', '{x+interval}', {buffer});")
    conn.commit()

  cursor.execute(f"SELECT COUNT(*) FROM {db_schema}.ab_routes;")
  pass1 = cursor.fetchone()[0] - pass0
  print(f"         : {pass1} paths")  
  
  # ------------------------------------------------------------------------------------------------
  # PASS 2
  # ------------------------------------------------------------------------------------------------
  for x in tqdm(range(0, ab_rows, interval), ncols=95, desc=f"{Fore.GREEN} - Pass 2: "):
    cursor.execute(f"SELECT {db_schema}.ab_routes_calc_2('{x}', '{x+interval}', {buffer});")
    conn.commit()

  cursor.execute(f"SELECT COUNT(*) FROM {db_schema}.ab_routes;")
  pass2 = cursor.fetchone()[0] - pass1 - pass0
  print(f"         : {pass2} paths")  
   
  # ------------------------------------------------------------------------------------------------
  # PASS 3
  # ------------------------------------------------------------------------------------------------
  for x in tqdm(range(0, ab_rows, interval), ncols=95, desc=f"{Fore.GREEN} - Pass 3: "):
    cursor.execute(f"SELECT {db_schema}.ab_routes_calc_3a('{x}', '{x+interval}', {buffer});")
    conn.commit()

  cursor.execute(f"SELECT COUNT(*) FROM {db_schema}.ab_routes;")
  pass3 = cursor.fetchone()[0] - pass2 - pass1 - pass0
  print(f"         : {pass3} paths")  

  # ------------------------------------------------------------------------------------------------
  # PASS 4
  # ------------------------------------------------------------------------------------------------
  for x in tqdm(range(0, ab_rows, interval), ncols=95, desc=f"{Fore.GREEN} - Pass 4: "):
    cursor.execute(f"SELECT {db_schema}.ab_routes_calc_3b('{x}', '{x+interval}', {buffer});")
    conn.commit()

  cursor.execute(f"SELECT COUNT(*) FROM {db_schema}.ab_routes;")
  pass4 = cursor.fetchone()[0] - pass3 - pass2 - pass1 - pass0
  print(f"         : {pass4} paths")  

  # ------------------------------------------------------------------------------------------------
  # PASS 5
  # ------------------------------------------------------------------------------------------------
  for x in tqdm(range(0, ab_rows, interval), ncols=95, desc=f"{Fore.GREEN} - Pass 5: "):
    cursor.execute(f"SELECT {db_schema}.ab_routes_calc_3ab('{x}', '{x+interval}', {buffer});")
    conn.commit()

  cursor.execute(f"SELECT COUNT(*) FROM {db_schema}.ab_routes;")
  pass5 = cursor.fetchone()[0] - pass4 - pass3 - pass2 - pass1 - pass0
  print(f"         : {pass5} paths")  
  
  # ------------------------------------------------------------------------------------------------
  # PASS 6
  # ------------------------------------------------------------------------------------------------
  for x in tqdm(range(0, ab_rows, interval), ncols=95, desc=f"{Fore.GREEN} - Pass 6: "):
    cursor.execute(f"SELECT {db_schema}.ab_routes_calc_4a('{x}', '{x+interval}', {buffer});")
    conn.commit()

  cursor.execute(f"SELECT COUNT(*) FROM {db_schema}.ab_routes;")
  pass6 = cursor.fetchone()[0] - pass5 - pass4 - pass3 - pass2 - pass1 - pass0
  print(f"         : {pass6} paths")  

  # ------------------------------------------------------------------------------------------------
  # PASS 7
  # ------------------------------------------------------------------------------------------------
  for x in tqdm(range(0, ab_rows, interval), ncols=95, desc=f"{Fore.GREEN} - Pass 7: "):
    cursor.execute(f"SELECT {db_schema}.ab_routes_calc_4b('{x}', '{x+interval}', {buffer});")
    conn.commit()

  cursor.execute(f"SELECT COUNT(*) FROM {db_schema}.ab_routes;")
  pass7 = cursor.fetchone()[0] - pass6 - pass5 - pass4 - pass3 - pass2 - pass1 - pass0
  print(f"         : {pass7} paths")  

  # ------------------------------------------------------------------------------------------------
  
  cursor.execute(f"SELECT COUNT(*) FROM {db_schema}.ab_routes;")
  abr_rows = cursor.fetchone()[0]
  diff = ab_rows - abr_rows

  print(f"{Fore.LIGHTGREEN_EX} - AB routes successfully calculated:            {abr_rows}")
  if diff != 0:
    print(f"{Fore.LIGHTRED_EX}   AB routes not calculated (unknown BUS stops): {diff}\n")

  print(95*f"{Fore.GREEN}-")
  cursor.execute(f"SELECT COUNT (*) FROM {db_schema}.ab_routes WHERE ST_GeometryType(geom) IN ('ST_LineString','ST_MultiLineString');")
  valid_geoms = cursor.fetchone()[0]
  print(f"{Fore.LIGHTGREEN_EX} - AB routes with valid linestring geometries: {valid_geoms}")

  cursor.execute(f"SELECT COUNT (*) FROM {db_schema}.ab_routes WHERE ST_GeometryType(geom) = 'ST_Point';")
  to_points = cursor.fetchone()[0]
  print(f"{Fore.LIGHTRED_EX} - AB routes converge to a point:                {to_points}")
  print(f"{Fore.LIGHTRED_EX}   (routes with extreme BUS stops positions)")
  
  cursor.execute(f"SELECT COUNT (*) FROM {db_schema}.ab_routes WHERE ST_GeometryType(geom) IS NULL OR ST_GeometryType(geom) = 'ST_GeometryCollection';")
  no_geoms = cursor.fetchone()[0]
  print(f"{Fore.LIGHTRED_EX} - AB routes with NULL geometry:                 {no_geoms}")
  print(f"{Fore.LIGHTRED_EX}   (nodes on a isolated network, not reachable)\n")

  print(f"{Fore.LIGHTGREEN_EX}Done!")

def routes_geom(conn, db_schema):
  '''
  routes_geom() will connect/merge all the lines that creates one route (with a unique BM_RouteID) 
  based on the RoutelinesUK file where each route has N pairs of lines of which it consists
  '''

  print(95*f"{Fore.GREEN}-")
  print(f"{Fore.LIGHTCYAN_EX}[Creating final route geometries]")
  
  cursor = conn.cursor()
  
  cursor.execute(f"DROP TABLE IF EXISTS {db_schema}.routes_geom")
  cursor.execute(f"CREATE TABLE {db_schema}.routes_geom(bmrouteID TEXT, geom geometry(Geometry,27700))")
  conn.commit()

  cursor.execute('''INSERT INTO ''' + db_schema + '''.routes_geom (bmrouteid, geom) 
                    SELECT rl.bmrouteid, ST_Union(rs.geom)
                    FROM( SELECT DISTINCT bmrouteid, anodestopid, bnodestopid FROM ''' + db_schema +'''.routelinesuk ORDER BY bmrouteid ) rl 
                      INNER JOIN ''' + db_schema + '''.ab_routes rs 
                        ON rl.anodestopid = rs.astopid AND rl.bnodestopid = rs.bstopid
                    GROUP BY rl.bmrouteid
                    ''')
  conn.commit()
  print(f"{Fore.LIGHTGREEN_EX}Done!")

def routelines_incomplete(conn, db_schema):
  
  print(95*f"{Fore.GREEN}-")
  print(f"{Fore.LIGHTCYAN_EX}[Creating a list of incomplete and missing routeline geometries]\n")

  cursor = conn.cursor()

  qry = f"""DROP TABLE IF EXISTS {db_schema}.routelines_incomplete;
            CREATE TABLE {db_schema}.routelines_incomplete(bm_route_id text, stops_missing integer) TABLESPACE pg_default;
            CREATE INDEX bm_route_id_idx ON {db_schema}.routelines_incomplete 
            USING btree (bm_route_id COLLATE pg_catalog."default" text_pattern_ops ASC NULLS LAST)TABLESPACE pg_default;
            INSERT INTO {db_schema}.routelines_incomplete 
            SELECT sub.bm_routeid, COUNT(*)
            FROM (SELECT mt.bm_routeid, st.naptanid, mt.stopid 
                FROM {db_schema}.maintable mt
                FULL OUTER JOIN {db_schema}.ptstops st 
                  ON mt.stopid = naptanid
                  WHERE naptanid IS NULL 
                GROUP BY mt.bm_routeid, st.naptanid, mt.stopid
            ) AS sub
            GROUP BY sub.bm_routeid
            ORDER BY COUNT(*) DESC;"""

  cursor.execute(qry)
  conn.commit()

  print(f"{Fore.LIGHTGREEN_EX}Done!")

def routelines_final(conn, db_schema, dir_shp, shp_out):
  '''Joining route geometries with freq. data and creating final table'''

  print(95*f"{Fore.GREEN}-")
  print(f"{Fore.LIGHTCYAN_EX}[Joining route geometries with freq. data and creating final table]")
  
  cursor = conn.cursor()
  
  cursor.execute(f"DROP TABLE IF EXISTS {db_schema}.routelines_final")
  cursor.execute('''CREATE TABLE ''' + db_schema + '''.routelines_final(
                  bm_routeid TEXT, 
                  operatorcode TEXT, 
                  servicenum TEXT, 
                  direction TEXT, 
                  operatorname TEXT,
                  monearly REAL, monam REAL, monbp REAL, monep REAL, monop REAL, monnight REAL, 
                  tueearly REAL,tueam REAL,  tuebp REAL, tueep REAL, tueop REAL, tuenight REAL,
                  wedearly REAL, wedam REAL, wedbp REAL, wedep REAL, wedop REAL, wednight REAL,
                  thurearly REAL, thuram REAL, thurbp REAL, thurep REAL, thurop REAL, thurnight REAL,
                  friearly REAL, friam REAL, fribp REAL, friep REAL, friop REAL, frinight REAL,
                  satearly REAL, satam REAL, satbp REAL, satep REAL, satop REAL, satnight REAL,
                  sunearly REAL, sunam REAL, sunbp REAL, sunep REAL, sunop REAL, sunnight REAL,
                  id SERIAL,
                  geom geometry(Geometry,27700)
                  ) TABLESPACE pg_default''')
  conn.commit()

  cursor.execute('''INSERT INTO ''' + db_schema + '''.routelines_final (bm_routeid,operatorcode,servicenum,direction,operatorname,monearly,monam,monbp,monep,monop,monnight,tueearly,tueam,tuebp,tueep,tueop,tuenight,wedearly,wedam,wedbp,wedep,wedop,wednight,thurearly,thuram,thurbp,thurep,thurop,thurnight,friearly,friam,fribp,friep,friop,frinight,satearly,satam,satbp,satep,satop,satnight,sunearly,sunam,sunbp,sunep,sunop,sunnight,id, geom)
                    SELECT rf.bm_routeid, rf.operatorcode, rf.servicenum, rf.direction, rf.operatorname, rf.monearly, rf.monam, rf.monbp, rf.monep, rf.monop, rf.monnight, rf.tueearly, rf.tueam, rf.tuebp, rf.tueep, rf.tueop, rf.tuenight, rf.wedearly, rf.wedam, rf.wedbp, rf.wedep, rf.wedop, rf.wednight, rf.thurearly, rf.thuram, rf.thurbp, rf.thurep, rf.thurop, rf.thurnight, rf.friearly, rf.friam, rf.fribp, rf.friep, rf.friop, rf.frinight, rf.satearly, rf.satam, rf.satbp, rf.satep, rf.satop, rf.satnight, rf.sunearly, rf.sunam, rf.sunbp, rf.sunep, rf.sunop, rf.sunnight, rf.id, rg.geom
                    FROM ''' + db_schema + '''.rlfrequk rf 
                    INNER JOIN ''' + db_schema + '''.routes_geom rg ON rf.bm_routeid = rg.bmrouteid''')
  conn.commit()
  
  # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  
  cursor.execute(f"SELECT COUNT(DISTINCT bmrouteid) FROM {db_schema}.routelinesuk;")
  rluk_rows = cursor.fetchone()[0]
  print(f"{Fore.LIGHTGREEN_EX} - Unique routelines in CIF file:                       {rluk_rows}")

  # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  qry1 = f"""SELECT COUNT(*) FROM {db_schema}.routelines_final rf WHERE EXISTS (SELECT bm_route_id FROM {db_schema}.routelines_incomplete ri WHERE ri.bm_route_id = rf.bm_routeid);"""
  cursor.execute(qry1)
  rluk_miss = cursor.fetchone()[0]
  print(f"{Fore.LIGHTRED_EX} - Incomplete routelines (with missing BUS stop gaps):    {rluk_miss}")

  # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  qry2 = f"""SELECT COUNT(*) FROM {db_schema}.routelines_final rf RIGHT JOIN {db_schema}.routelines_incomplete ri ON rf.bm_routeid = ri.bm_route_id WHERE rf.bm_routeid IS NULL"""
  cursor.execute(qry2)

  rluk_none = cursor.fetchone()[0]
  print(f"{Fore.LIGHTRED_EX} - Missing routelines (no route geometry at all):         {rluk_none}")
  
  # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  sql = f"""COPY (SELECT ri.bm_route_id FROM {db_schema}.routelines_final rf RIGHT JOIN {db_schema}.routelines_incomplete ri ON rf.bm_routeid = ri.bm_route_id 
            WHERE rf.bm_routeid IS NULL) TO STDOUT WITH CSV DELIMITER ';'"""
  with open(f"{dir_shp}{shp_out}_missing.csv", "w") as file:
      cursor.copy_expert(sql, file)
  print(f"{Fore.LIGHTRED_EX}\n   The list of missing routelines exported to [{dir_shp}{shp_out}_missing.csv]\n")
  
  
  print(f"{Fore.LIGHTGREEN_EX}Done!")

