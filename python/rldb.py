# ============================================================================
# database setup
# ============================================================================

import sys

# from getpass import getpass
# from termcolor import colored
from colorama import init
init(autoreset=True)
from colorama import Fore

import psycopg2
from psycopg2.extras import LoggingConnection

from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists

import pandas as pd

from setup import dir_cif, idx_no

# import logging

from setup import db_schema, shp_table

def db_connect(db_server, db_port, dbname, db_user, db_password):

    # logging.basicConfig()
    # logging.getLogger('sqlalchemy.dialects.postgresql').setLevel(logging.INFO)

    # db_password = getpass(f"Please enter password for user {db_user} @ database {dbname}:")

    # db connection:
    engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_password}@{db_server}:{db_port}/{dbname}") # , echo=True

    if database_exists(engine.url):
        return engine
    else:
        return 0


def connect(db_name, db_user, db_password, db_server, db_port):
    """ Connect to the PostgreSQL database server """

    # logging.basicConfig()
    # logging.basicConfig(level=logging.INFO)
    # logger = logging.getLogger("RL")

    conn = None
    conn = psycopg2.connect(dbname=db_name, user=db_user, password=db_password, host=db_server, port=db_port) # connection_factory=LoggingConnection, 
    return conn

def noc_table(conn, dir_cif, db_schema):
    NOCTable = pd.read_csv(dir_cif + 'NOCTable.csv')
    NOCTable.to_sql('noctable', conn, schema=db_schema, if_exists='replace', index = False, chunksize = 100000)
    conn.execute('''CREATE UNIQUE INDEX ''' + idx_no + '''_noccode_idx ON ''' + db_schema + '''.noctable USING btree ("NOCCODE" text_pattern_ops ASC NULLS LAST) TABLESPACE pg_default''')


def db_setup(db_server, db_port, db_name, db_def, db_user, db_password):
    ask_cont = input(f"{Fore.YELLOW}Do you want to create project database '{db_name}' on PostgreSQL sever '{db_server}'? (y/n)")
    if ask_cont == "y":
        print(f"{Fore.CYAN}Connecting to PostgreSQL default instance...")
        try:        
            def_db_engine = db_connect(db_server, db_port, db_def, db_user, db_password)
            conn = def_db_engine.connect()
            conn.execute("COMMIT")
            print(f"{Fore.CYAN}Creating project database '{db_name}'...")
            conn.execute(f"CREATE DATABASE {db_name}")
            conn.close()
            print(f"{Fore.GREEN}Project database '{db_name}' succesfully created !")

            print("-")
            print(f"{Fore.CYAN}[Project database configuration]")
            
            # sqlalchemy
            # print(f"{Fore.CYAN}Connecting to '{db_name}' database:")
            rl_engine = db_connect(db_server, db_port, db_name, db_user, db_password)
            conn2 = rl_engine.connect()
         
            # psycopg2
            print(f"{Fore.CYAN}Connecting to '{db_name}' database:")

            rl_conn2 = connect(db_name, db_user, db_password, db_server, db_port)
            cursor = rl_conn2.cursor()

            print(f"{Fore.CYAN} Installing extensions...")
            cursor.execute('CREATE EXTENSION postgis')
            rl_conn2.commit()

            cursor.execute('CREATE EXTENSION pgrouting')
            rl_conn2.commit()

            cursor.execute('CREATE EXTENSION postgis_sfcgal')
            rl_conn2.commit()

            print(f"{Fore.CYAN} Creating project schema [{db_schema}]...")
            cursor.execute(f"CREATE SCHEMA {db_schema} AUTHORIZATION postgres")
            rl_conn2.commit()

            print(f"{Fore.CYAN} Importing NOC table...")
            noc_table(conn2, dir_cif, db_schema)

            print(f"{Fore.CYAN} Creating '{db_schema}.ptstops_temp' table...")
            cursor.execute(f"CREATE TABLE {db_schema}.ptstops_temp(naptanid text, xcoord text, ycoord text)")
            rl_conn2.commit()

            # cursor.close()

            print(f"{Fore.GREEN}Done!")

        except Exception as error:
            print(f"{Fore.LIGHTRED_EX}Could not create project database '{db_name}'!")
            print(f"{Fore.LIGHTRED_EX}{error}")

def db_func(db_server, db_port, db_name, db_schema, db_user, db_password, shp_table):

    print(f"{Fore.CYAN} Creating Routelines functions ...")
    
    # Stored procedures definitions (25)

    ab_routes_calc_0 = f"""CREATE OR REPLACE FUNCTION {db_schema}.ab_routes_calc_0(
            _val1 text,
            _val2 text,
            _buffer integer)
            RETURNS text
            LANGUAGE 'plpgsql'
            COST 100
            VOLATILE PARALLEL UNSAFE
        AS $BODY$
        DECLARE
        BEGIN
            EXECUTE 'INSERT INTO {db_schema}.ab_routes
                    SELECT *
                    FROM (SELECT anodestopid, bnodestopid, anodexcoord, anodeycoord, bnodexcoord, bnodeycoord,
                        (ST_LineSubstring((SELECT geom FROM {db_schema}.{shp_table} WHERE gid = a_edge), LEAST(a_fraction, b_fraction),GREATEST(a_fraction, b_fraction))),
                        id, a_fraction, b_fraction, a_edge, b_edge, a_node, b_node, hops,' || ($3) ||' AS buffer_m, a_edge_circ, b_edge_circ
                        FROM {db_schema}.abnodesuk
                        WHERE anodexcoord != 0 AND bnodexcoord != 0
                            AND a_edge = b_edge
                            AND id >= ' || ($1) || ' AND id < ' || ($2) || '
                            ) t;';
            RETURN 'Done!';
        END
        $BODY$;"""

    ab_routes_calc_1 = f"""CREATE OR REPLACE FUNCTION {db_schema}.ab_routes_calc_1(
            _val1 text,
            _val2 text,
            _buffer integer)
            RETURNS text
            LANGUAGE 'plpgsql'
            COST 100
            VOLATILE PARALLEL UNSAFE
        AS $BODY$
        DECLARE
        BEGIN
        EXECUTE 'INSERT INTO {db_schema}.ab_routes
                SELECT *
                FROM (SELECT anodestopid, bnodestopid, anodexcoord, anodeycoord, bnodexcoord, bnodeycoord,
                    (SELECT geom_ FROM {db_schema}.get_shortest_path_1(anodestopid, bnodestopid, a_node, b_node,' || ($3) ||')),
                    id, a_fraction, b_fraction, a_edge, b_edge, a_node, b_node, hops,' || ($3) ||' AS buffer_m, a_edge_circ, b_edge_circ
                    FROM {db_schema}.abnodesuk
                    WHERE anodexcoord != 0 AND bnodexcoord != 0
                        AND a_edge != b_edge 
                        AND (a_fraction = 0 OR a_fraction = 1)
                        AND (b_fraction = 0 OR b_fraction = 1) 
                        AND id >= ' || ($1) || ' AND id < ' || ($2) || '
                        ) t;
                UPDATE {db_schema}.ab_routes 
                SET geom = (SELECT geom_ FROM {db_schema}.get_shortest_path_1(astopid, bstopid, a_node, b_node,' || 20*($3) ||'))
                    ,buffer_m = ' || 20*($3) ||'
                WHERE (geom IS NULL OR ST_GeometryType(geom) = ''ST_GeometryCollection'')
                AND a_edge != b_edge 
                AND (a_fraction = 0 OR a_fraction = 1)
                AND (b_fraction = 0 OR b_fraction = 1)
                AND id >= ' || ($1) || ' AND id < ' || ($2) || '
                ;';
            RETURN 'Done!';
            END
        $BODY$;"""

    ab_routes_calc_2 = f"""CREATE OR REPLACE FUNCTION {db_schema}.ab_routes_calc_2(
            _val1 text,
            _val2 text,
            _buffer integer)
            RETURNS text
            LANGUAGE 'plpgsql'
            COST 100
            VOLATILE PARALLEL UNSAFE
        AS $BODY$
        DECLARE
        BEGIN
        EXECUTE 'INSERT INTO {db_schema}.ab_routes
                SELECT *
                FROM (SELECT anodestopid, bnodestopid, anodexcoord, anodeycoord, bnodexcoord, bnodeycoord,
                    (SELECT geom_ FROM {db_schema}.get_shortest_path_2(anodestopid, bnodestopid,' || ($3) ||')),
                    id, a_fraction, b_fraction, a_edge, b_edge, a_node, b_node, hops,' || ($3) ||' AS buffer_m, a_edge_circ, b_edge_circ
                    FROM {db_schema}.abnodesuk
                    WHERE anodexcoord != 0 AND bnodexcoord != 0
                        AND a_edge != b_edge
                        AND (a_fraction > 0 AND a_fraction < 1) 
                        AND (b_fraction > 0 AND b_fraction < 1) 
                        AND id >= ' || ($1) || ' AND id < ' || ($2) || '
                        ) t;
                UPDATE {db_schema}.ab_routes 
                SET geom = (SELECT geom_ FROM {db_schema}.get_shortest_path_2(astopid, bstopid,' || 20*($3) ||'))
                    ,buffer_m = ' || 20*($3) ||'
                WHERE (geom IS NULL OR ST_GeometryType(geom) = ''ST_GeometryCollection'')
                AND a_edge != b_edge
                AND (a_fraction > 0 AND a_fraction < 1)
                AND (b_fraction > 0 AND b_fraction < 1)
                AND id >= ' || ($1) || ' AND id < ' || ($2) || '
                ;';
        RETURN 'Done!';
        END
        $BODY$;"""

    ab_routes_calc_3a = f"""CREATE OR REPLACE FUNCTION {db_schema}.ab_routes_calc_3a(
            _val1 text,
            _val2 text,
            _buffer integer)
            RETURNS text
            LANGUAGE 'plpgsql'
            COST 100
            VOLATILE PARALLEL UNSAFE
        AS $BODY$
        DECLARE
        BEGIN
        EXECUTE 'INSERT INTO {db_schema}.ab_routes
                SELECT *
                FROM (SELECT anodestopid, bnodestopid, anodexcoord, anodeycoord, bnodexcoord, bnodeycoord,
                    (SELECT geom_ FROM {db_schema}.get_shortest_path_3a(anodestopid, bnodestopid,' || ($3) ||')),
                    id, a_fraction, b_fraction, a_edge, b_edge, a_node, b_node, hops,' || ($3) ||' AS buffer_m, a_edge_circ, b_edge_circ
                    FROM {db_schema}.abnodesuk
                    WHERE anodexcoord != 0 AND bnodexcoord != 0 
                        AND a_edge != b_edge
                        AND a_node != b_node
                        AND (a_fraction > 0 AND a_fraction < 1)
                        AND (b_fraction = 0 OR b_fraction = 1) 
                        AND (a_edge_circ IS NULL)
                        AND id >= ' || ($1) || ' AND id < ' || ($2) || '
                        ) t;
                UPDATE {db_schema}.ab_routes 
                SET geom = (SELECT geom_ FROM {db_schema}.get_shortest_path_3a(astopid, bstopid,' || 20*($3) ||'))
                ,buffer_m = ' || 20*($3) ||'
                    WHERE (geom IS NULL OR ST_GeometryType(geom) = ''ST_GeometryCollection'')
                    AND a_edge != b_edge
                    AND a_node != b_node
                    AND (a_fraction > 0 AND a_fraction < 1)
                    AND (b_fraction = 0 OR b_fraction = 1) 
                    AND a_edge_circ IS NULL
                    AND id >= ' || ($1) || ' AND id < ' || ($2) || ';';
        RETURN 'Done!';
        END
        $BODY$;"""

    ab_routes_calc_3ab = f"""CREATE OR REPLACE FUNCTION {db_schema}.ab_routes_calc_3ab(
            _val1 text,
            _val2 text,
            _buffer integer)
            RETURNS text
            LANGUAGE 'plpgsql'
            COST 100
            VOLATILE PARALLEL UNSAFE
        AS $BODY$
        DECLARE
        BEGIN
            EXECUTE 'INSERT INTO {db_schema}.ab_routes
                    SELECT *
                    FROM (SELECT anodestopid, bnodestopid, anodexcoord, anodeycoord, bnodexcoord, bnodeycoord,
                        (SELECT geom_ FROM {db_schema}.get_shortest_path_3ab(anodestopid, bnodestopid,' || ($3) ||')),
                        id, a_fraction, b_fraction, a_edge, b_edge, a_node, b_node, hops,' || ($3) ||' AS buffer_m, a_edge_circ, b_edge_circ
                        FROM {db_schema}.abnodesuk
                        WHERE anodexcoord != 0 AND bnodexcoord != 0 
                        AND a_edge != b_edge 
                        AND a_node = b_node
                            AND ((a_fraction > 0 AND a_fraction < 1) AND (b_fraction = 0 OR b_fraction = 1) 
                            OR (a_fraction = 0 OR a_fraction = 1)  AND (b_fraction > 0 AND b_fraction < 1))
                        AND id >= ' || ($1) || ' AND id < ' || ($2) || '
                            ) t;
                    UPDATE {db_schema}.ab_routes 
                    SET geom = (SELECT geom_ FROM {db_schema}.get_shortest_path_3ab(astopid, bstopid,' || 20*($3) ||'))
                    ,buffer_m = ' || 20*($3) ||'
                        WHERE (geom IS NULL OR ST_GeometryType(geom) = ''ST_GeometryCollection'')
                        AND a_edge != b_edge 
                        AND a_node = b_node
                            AND ((a_fraction > 0 AND a_fraction < 1) AND (b_fraction = 0 OR b_fraction = 1) 
                            OR (a_fraction = 0 OR a_fraction = 1)  AND (b_fraction > 0 AND b_fraction < 1))
                            AND id >= ' || ($1) || ' AND id < ' || ($2) || ';';
        RETURN 'Done!';
        END
        $BODY$;"""

    ab_routes_calc_3b = f"""CREATE OR REPLACE FUNCTION {db_schema}.ab_routes_calc_3b(
            _val1 text,
            _val2 text,
            _buffer integer)
            RETURNS text
            LANGUAGE 'plpgsql'
            COST 100
            VOLATILE PARALLEL UNSAFE
        AS $BODY$
        DECLARE
        BEGIN
            EXECUTE 'INSERT INTO {db_schema}.ab_routes
                    SELECT *
                    FROM (SELECT anodestopid, bnodestopid, anodexcoord, anodeycoord, bnodexcoord, bnodeycoord,
                        (SELECT geom_ FROM {db_schema}.get_shortest_path_3b(anodestopid, bnodestopid,' || ($3) ||')),
                        id, a_fraction, b_fraction, a_edge, b_edge, a_node, b_node, hops,' || ($3) ||' AS buffer_m, a_edge_circ, b_edge_circ
                        FROM {db_schema}.abnodesuk
                        WHERE anodexcoord != 0 AND bnodexcoord != 0 
                            AND a_edge != b_edge
                            AND a_node != b_node
                            AND (a_fraction = 0 OR a_fraction = 1) 
                            AND (b_fraction > 0 AND b_fraction < 1)
                            AND (b_edge_circ IS NULL)
                            AND id >= ' || ($1) || ' AND id < ' || ($2) || '
                            ) t;
                    UPDATE {db_schema}.ab_routes 
                            SET geom = (SELECT geom_ FROM {db_schema}.get_shortest_path_3b(astopid, bstopid,' || 20*($3) ||'))
                                ,buffer_m = ' || 20*($3) ||'
                                WHERE (geom IS NULL OR ST_GeometryType(geom) = ''ST_GeometryCollection'')
                                AND a_edge != b_edge
                                AND a_node != b_node
                                AND (a_fraction = 0 OR a_fraction = 1) 
                                AND (b_fraction > 0 AND b_fraction < 1)
                                AND (b_edge_circ IS NULL)
                                AND id >= ' || ($1) || ' AND id < ' || ($2) || ';
                    ';
            RETURN 'Done!';
        END
        $BODY$;"""

    ab_routes_calc_4a = f"""CREATE OR REPLACE FUNCTION {db_schema}.ab_routes_calc_4a(
            _val1 text,
            _val2 text,
            _buffer integer)
            RETURNS text
            LANGUAGE 'plpgsql'
            COST 100
            VOLATILE PARALLEL UNSAFE
        AS $BODY$
        DECLARE
        BEGIN
            EXECUTE 'INSERT INTO {db_schema}.ab_routes
                    SELECT *
                    FROM (SELECT anodestopid, bnodestopid, anodexcoord, anodeycoord, bnodexcoord, bnodeycoord,
                        (SELECT geom_ FROM {db_schema}.get_shortest_path_4a(anodestopid, bnodestopid,' || ($3) ||')),
                        id, a_fraction, b_fraction, a_edge, b_edge, a_node, b_node, hops,' || ($3) ||' AS buffer_m, a_edge_circ, b_edge_circ
                        FROM {db_schema}.abnodesuk
                        WHERE anodexcoord != 0 AND bnodexcoord != 0 
                            AND a_edge != b_edge
                            AND (a_fraction > 0 AND a_fraction < 1)
                            AND (b_fraction = 0 OR b_fraction = 1) 
                            AND (a_edge_circ IS NOT NULL)
                            AND id >= ' || ($1) || ' AND id < ' || ($2) || '
                            ) t;
                    UPDATE {db_schema}.ab_routes 
                    SET geom = (SELECT geom_ FROM {db_schema}.get_shortest_path_4a(astopid, bstopid,' || 20*($3) ||'))
                    ,buffer_m = ' || 20*($3) ||'
                        WHERE (geom IS NULL OR ST_GeometryType(geom) = ''ST_GeometryCollection'')
                        AND a_edge != b_edge
                        AND (a_fraction > 0 AND a_fraction < 1)
                        AND (b_fraction = 0 OR b_fraction = 1) 
                        AND (a_edge_circ IS NOT NULL)
                        AND id >= ' || ($1) || ' AND id < ' || ($2) || ';
                    ';
            RETURN 'Done!';
        END
        $BODY$;"""

    ab_routes_calc_4b = f"""CREATE OR REPLACE FUNCTION {db_schema}.ab_routes_calc_4b(
            _val1 text,
            _val2 text,
            _buffer integer)
            RETURNS text
            LANGUAGE 'plpgsql'
            COST 100
            VOLATILE PARALLEL UNSAFE
        AS $BODY$
        DECLARE
        BEGIN
            EXECUTE 'INSERT INTO {db_schema}.ab_routes
                    SELECT *
                    FROM (SELECT anodestopid, bnodestopid, anodexcoord, anodeycoord, bnodexcoord, bnodeycoord,
                        (SELECT geom_ FROM {db_schema}.get_shortest_path_4b(anodestopid, bnodestopid,' || ($3) ||')),
                        id, a_fraction, b_fraction, a_edge, b_edge, a_node, b_node, hops,' || ($3) ||' AS buffer_m, a_edge_circ, b_edge_circ
                        FROM {db_schema}.abnodesuk
                        WHERE anodexcoord != 0 AND bnodexcoord != 0 
                            AND a_edge != b_edge
                            AND (a_fraction = 0 OR a_fraction = 1) 
                            AND (b_fraction > 0 AND b_fraction < 1)
                            AND (b_edge_circ IS NOT NULL)
                            AND id >= ' || ($1) || ' AND id < ' || ($2) || '
                            ) t;
                    UPDATE {db_schema}.ab_routes 
                        SET geom = (SELECT geom_ FROM {db_schema}.get_shortest_path_4b(astopid, bstopid,' || 20*($3) ||'))
                            ,buffer_m = ' || 20*($3) ||'
                            WHERE (geom IS NULL OR ST_GeometryType(geom) = ''ST_GeometryCollection'')
                            AND a_edge != b_edge
                            AND (a_fraction = 0 OR a_fraction = 1) 
                            AND (b_fraction > 0 AND b_fraction < 1)
                            AND (b_edge_circ IS NOT NULL)
                            AND id >= ' || ($1) || ' AND id < ' || ($2) || ';
                    ';
            RETURN 'Done!';
        END
        $BODY$;"""

    ab_routes_table = f"""CREATE OR REPLACE FUNCTION {db_schema}.ab_routes_table(
            _schema text,
            _table text)
            RETURNS text
            LANGUAGE 'plpgsql'
            COST 100
            VOLATILE PARALLEL UNSAFE
        AS $BODY$
        DECLARE
        _qual_tbl text := concat_ws('.', quote_ident(_schema), quote_ident(_table));
        _row_found bool;
        BEGIN
        IF to_regclass(_qual_tbl) IS NOT NULL THEN
            EXECUTE 'SELECT EXISTS (SELECT FROM ' || _qual_tbl || ')'
            INTO _row_found;
            IF _row_found THEN
                EXECUTE 'TRUNCATE ' || _qual_tbl;
                RETURN 'Table truncated: ' || _qual_tbl;
            ELSE  
                RETURN 'Table exists but is empty: ' || _qual_tbl;
            END IF;
        ELSE  
            CREATE TABLE {db_schema}.ab_routes
            (
                astopid character varying(20),
                bstopid character varying(20),
                axcoord integer,
                aycoord integer,
                bxcoord integer,
                bycoord integer,
                geom geometry(Geometry, 27700),
                id integer,
                a_fraction double precision,
                b_fraction double precision,
                a_edge integer,
                b_edge integer,
                a_node integer,
                b_node integer,
                hops integer,
                buffer_m integer,
                a_edge_circ integer,
                b_edge_circ integer
            );
            RETURN 'Table created: ' || _qual_tbl;
        END IF;
        END
        $BODY$;"""

    fraction_calc = f"""CREATE OR REPLACE FUNCTION {db_schema}.fraction_calc(
            road integer,
            bus_stop character varying)
            RETURNS double precision
            LANGUAGE 'plpgsql'
            COST 100
            VOLATILE PARALLEL UNSAFE
        AS $BODY$
        BEGIN
        RETURN (WITH line_g AS (SELECT geom FROM {db_schema}.{shp_table} WHERE gid = $1),
                    point_g AS (SELECT geom FROM {db_schema}.nodesuk WHERE stop_id = $2)
                    SELECT ST_LineLocatePoint((SELECT geom from line_g), (SELECT geom FROM point_g))
                );
        END
        $BODY$;"""

    get_edge_by_node = f"""CREATE OR REPLACE FUNCTION {db_schema}.get_edge_by_node(
            integer)
            RETURNS geometry
            LANGUAGE 'plpgsql'
            COST 100
            VOLATILE PARALLEL UNSAFE
        AS $BODY$
        BEGIN
            RETURN (SELECT r.geom AS geom 
                    FROM {db_schema}.{shp_table} r 
                    WHERE gid = (SELECT edge_id 
                                FROM {db_schema}.nodesuk 
                                WHERE id_pk = $1));
        END
        $BODY$;"""

    get_edge_by_stopid = f"""CREATE OR REPLACE FUNCTION {db_schema}.get_edge_by_stopid(
            character varying)
            RETURNS geometry
            LANGUAGE 'plpgsql'
            COST 100
            VOLATILE PARALLEL UNSAFE
        AS $BODY$
        BEGIN
            RETURN (SELECT r.geom AS geom 
                    FROM {db_schema}.{shp_table} r 
                    WHERE gid = (SELECT edge_id 
                                FROM {db_schema}.nodesuk 
                                WHERE stop_id = $1)
                );
        END
        $BODY$;"""

    get_nearest_edge_1 = f"""CREATE OR REPLACE FUNCTION {db_schema}.get_nearest_edge_1(
            node character varying)
            RETURNS integer
            LANGUAGE 'plpgsql'
            COST 100
            VOLATILE PARALLEL UNSAFE
        AS $BODY$
        BEGIN
        RETURN (SELECT road.gid
                FROM {db_schema}.{shp_table} road
                ORDER BY road.geom <-> (SELECT {db_schema}.get_node_geom($1))
                LIMIT 1);
        END
        $BODY$;"""

    get_nearest_edge_2 = f"""CREATE OR REPLACE FUNCTION {db_schema}.get_nearest_edge_2(
            node character varying,
            graph_node bigint)
            RETURNS integer
            LANGUAGE 'plpgsql'
            COST 100
            VOLATILE PARALLEL UNSAFE
        AS $BODY$
        BEGIN
        RETURN (SELECT road.gid
                FROM {db_schema}.{shp_table} road
                WHERE road.source = $2
                OR road.target = $2
                ORDER BY road.geom <-> (SELECT {db_schema}.get_node_geom($1))
                LIMIT 1);
        END
        $BODY$;"""

    get_nearest_node_1 = f"""CREATE OR REPLACE FUNCTION {db_schema}.get_nearest_node_1(
            node character varying)
            RETURNS integer
            LANGUAGE 'plpgsql'
            COST 100
            VOLATILE PARALLEL UNSAFE
        AS $BODY$
        DECLARE

        edge_id int := (SELECT {db_schema}.get_nearest_edge_1($1));

        BEGIN
        RETURN (SELECT n.id
                FROM {db_schema}.{shp_table}_vertices_pgr n
                WHERE n.id IN (SELECT source FROM {db_schema}.{shp_table} WHERE gid = edge_id)
                OR n.id IN (SELECT target FROM {db_schema}.{shp_table} WHERE gid = edge_id)
                ORDER BY n.the_geom <-> (SELECT {db_schema}.get_node_geom($1))
                LIMIT 1);
        END
        $BODY$;"""

    get_nearest_node_2 = f"""CREATE OR REPLACE FUNCTION {db_schema}.get_nearest_node_2(
            node character varying)
            RETURNS TABLE(id_n bigint) 
            LANGUAGE 'plpgsql'
            COST 100
            VOLATILE PARALLEL UNSAFE
            ROWS 1000

        AS $BODY$
        DECLARE

        edge_id int := (SELECT {db_schema}.get_nearest_edge_1($1));

        BEGIN
        RETURN QUERY 
            (SELECT n.id
                FROM {db_schema}.{shp_table}_vertices_pgr n
                WHERE n.id IN (SELECT source FROM {db_schema}.{shp_table} WHERE gid = edge_id)
                OR n.id IN (SELECT target FROM {db_schema}.{shp_table} WHERE gid = edge_id)
                ORDER BY n.the_geom <-> (SELECT {db_schema}.get_node_geom($1))
                LIMIT 2);
        END
        $BODY$;"""

    get_node_geom = f"""CREATE OR REPLACE FUNCTION {db_schema}.get_node_geom(
            node character varying)
            RETURNS geometry
            LANGUAGE 'plpgsql'
            COST 100
            VOLATILE PARALLEL UNSAFE
        AS $BODY$
        BEGIN
            RETURN (SELECT geom	FROM {db_schema}.nodesuk WHERE stop_id = $1);
        END
        $BODY$;"""

    get_point_by_node = f"""CREATE OR REPLACE FUNCTION {db_schema}.get_point_by_node(
            integer)
            RETURNS geometry
            LANGUAGE 'plpgsql'
            COST 100
            VOLATILE PARALLEL UNSAFE
        AS $BODY$
        BEGIN
            RETURN (SELECT geom FROM {db_schema}.nodesuk WHERE id_pk = $1);
        END
        $BODY$;"""

    get_shortest_path_1 = f"""CREATE OR REPLACE FUNCTION {db_schema}.get_shortest_path_1(
            a_node character varying,
            b_node character varying,
            a_nearest integer,
            b_nearest integer,
            buffer integer)
            RETURNS TABLE(geom_ geometry) 
            LANGUAGE 'plpgsql'
            COST 100
            VOLATILE PARALLEL UNSAFE
            ROWS 1000

        AS $BODY$
        DECLARE 
        geom_a text := (SELECT ST_AsTEXT(geom) FROM {db_schema}.nodesuk WHERE stop_id = $1);
        geom_b text := (SELECT ST_AsTEXT(geom) FROM {db_schema}.nodesuk WHERE stop_id = $2);

        squery text :='SELECT ST_Union(geom) as geom_ FROM {db_schema}.{shp_table} 
            JOIN(SELECT seq, node, edge, cost FROM pgr_dijkstra(''SELECT gid as id, source, target, st_length(geom) as cost	FROM {db_schema}.{shp_table} 
            WHERE geom && ST_Expand(ST_Envelope(ST_Collect(ST_GeomFromText(''''' || geom_a || '''''), ST_GeomFromText(''''' || geom_b ||'''''))),'''''||$5||''''')''' || ', ' || $3 || ', ' || $4 || ',false)) AS route 
            ON {db_schema}.{shp_table}.gid = route.edge';

        BEGIN 
            RETURN QUERY 
            EXECUTE squery USING a_node, b_node, geom_a, geom_b, a_nearest, b_nearest;
        END
        $BODY$;"""

    get_shortest_path_2  = f"""CREATE OR REPLACE FUNCTION {db_schema}.get_shortest_path_2(
            node_a character varying,
            node_b character varying,
            buffer integer)
            RETURNS TABLE(geom_ geometry) 
            LANGUAGE 'plpgsql'
            COST 100
            VOLATILE PARALLEL UNSAFE
            ROWS 1000

        AS $BODY$
        DECLARE     

        a_node_id int := (SELECT id_pk FROM {db_schema}.nodesuk WHERE stop_id = $1);
        b_node_id int := (SELECT id_pk FROM {db_schema}.nodesuk WHERE stop_id = $2);

        a_node text := (SELECT ST_AsTEXT(geom) FROM {db_schema}.nodesuk WHERE stop_id = $1);
        b_node text := (SELECT ST_AsTEXT(geom) FROM {db_schema}.nodesuk WHERE stop_id = $2);

        q1 text :='WITH route AS (SELECT * FROM pgr_withPoints(''SELECT gid as id, source, target, st_length(geom) as cost FROM {db_schema}.{shp_table} 
        WHERE geom && ST_Expand(ST_Envelope(ST_Collect(ST_GeomFromText(''''' || a_node || '''''), ST_GeomFromText(''''' || b_node ||'''''))),'||$3||')''' || ',' ||E'
        ' || '''SELECT id_pk as pid, edge_id, fraction FROM {db_schema}.nodesuk 
        WHERE geom && ST_Expand(ST_Envelope(ST_Collect(ST_GeomFromText(''''' || a_node || '''''), ST_GeomFromText(''''' || b_node ||'''''))),'||$3||')'', -' || a_node_id || ', -' || b_node_id || ', directed => false)),';
        q2 text := E'' || 'start_edge AS (SELECT geom FROM {db_schema}.{shp_table} WHERE gid = (SELECT edge FROM route WHERE path_seq = 1)),';
        q3 text := E'' || 'start_frac_1 AS (SELECT ST_LineLocatePoint((SELECT geom FROM start_edge),(SELECT {db_schema}.get_node_geom('''|| $1 ||''')))),';
        q4 text := E'' || 'start_frac_2 AS (SELECT ST_LineLocatePoint((SELECT geom FROM start_edge),(SELECT the_geom FROM {db_schema}.{shp_table}_vertices_pgr WHERE id = (SELECT node FROM route WHERE path_seq = (SELECT MIN(path_seq)+1 FROM route))))),';
        q5 text := E'' || 'start_geom AS (SELECT ST_LineSubstring((SELECT geom FROM start_edge),(SELECT LEAST ((SELECT * FROM start_frac_1),(SELECT * FROM start_frac_2))),(SELECT GREATEST ((SELECT * FROM start_frac_1),(SELECT * FROM start_frac_2))))),';
        q6 text := E'' || 'end_edge AS (SELECT geom FROM {db_schema}.{shp_table} WHERE gid = (SELECT edge FROM route WHERE path_seq = (SELECT MAX(path_seq)-1 FROM route))),';
        q7 text := E'' || 'end_frac_1 AS (SELECT ST_LineLocatePoint((SELECT geom FROM end_edge),(SELECT {db_schema}.get_node_geom('''|| $2 ||''')))),';
        q8 text := E'' || 'end_frac_2 AS (SELECT ST_LineLocatePoint((SELECT geom FROM end_edge),(SELECT the_geom FROM {db_schema}.{shp_table}_vertices_pgr WHERE id = (SELECT node FROM route WHERE path_seq = (SELECT MAX(path_seq)-1 FROM route))))),';
        q9 text := E'' || 'end_geom AS (SELECT ST_LineSubstring((SELECT geom FROM end_edge),(SELECT LEAST ((SELECT * FROM end_frac_1),(SELECT * FROM end_frac_2))),(SELECT GREATEST ((SELECT * FROM end_frac_1),(SELECT * FROM end_frac_2))))),';
        q10 text := E'' || 'middle_edges AS (SELECT * FROM {db_schema}.{shp_table}, route WHERE {db_schema}.{shp_table}.gid=route.edge AND route.path_seq BETWEEN (SELECT MIN(route.path_seq)+1 FROM route) AND (SELECT MAX(route.path_seq)-2  FROM route)),';
        q11 text := E'' || 'middle_geom AS (SELECT ST_LineMerge(ST_Union(geom)) AS geom FROM middle_edges)';
        q12 text := E'' || 'SELECT ST_LineMerge(ST_Union(ARRAY[(SELECT * FROM start_geom),(SELECT * FROM middle_geom),(SELECT * FROM end_geom)]));';

        query_ text:= q1 || q2 || q3 || q4 || q5 || q6 || q7 || q8  || q9 || q10 || q11 || q12;

        BEGIN 
            RETURN QUERY 
            EXECUTE query_ USING $1, $2, $3;
        END
        $BODY$;"""

    get_shortest_path_3a = f"""CREATE OR REPLACE FUNCTION {db_schema}.get_shortest_path_3a(
            a_node character varying,
            b_node character varying,
            buffer integer)
            RETURNS TABLE(geom_ geometry) 
            LANGUAGE 'plpgsql'
            COST 100
            VOLATILE PARALLEL UNSAFE
            ROWS 1000

        AS $BODY$
        DECLARE 

        geom_a text := (SELECT ST_AsTEXT(geom) FROM {db_schema}.nodesuk WHERE stop_id = $1);
        geom_b text := (SELECT ST_AsTEXT(geom) FROM {db_schema}.nodesuk WHERE stop_id = $2);

        a_node_id int := (SELECT node_id FROM {db_schema}.nodesuk WHERE stop_id = $1);
        b_node_id int := (SELECT node_id FROM {db_schema}.nodesuk WHERE stop_id = $2);

        a_edge_id int := (SELECT {db_schema}.get_nearest_edge_2($1, a_node_id));

        q1 text :='WITH route AS (SELECT * FROM {db_schema}.{shp_table} JOIN(SELECT * FROM pgr_dijkstra(''SELECT gid as id, source, target, st_length(geom) as cost FROM {db_schema}.{shp_table} WHERE geom && ST_Expand(ST_Envelope(ST_Collect(ST_GeomFromText(''''' || geom_a || '''''), ST_GeomFromText(''''' || geom_b ||'''''))),'||$3||')''' ||', ' || a_node_id || ', ' || b_node_id || ',false)) AS route ON {db_schema}.{shp_table}.gid = route.edge),';
        q2 text := E'' || 'end_route AS (SELECT CASE WHEN (SELECT source FROM route WHERE path_seq = 1) = ' || a_node_id || '
                            -- 1
                                        THEN ( SELECT CASE WHEN (SELECT edge FROM route WHERE path_seq = 1)=' || a_edge_id || ' 
                                                            -- a
                                                            THEN (SELECT ST_LineMerge(ST_Union(geom)) FROM route WHERE path_seq > 1)
                                                            -- b
                                                            ELSE (SELECT ST_LineMerge(ST_Union(geom)) FROM route) 
                                                            END)
                            -- 2
                                        ELSE ( SELECT CASE WHEN (SELECT edge FROM route WHERE path_seq = 1)=' || a_edge_id || '
                                                            -- a
                                                            THEN (SELECT ST_LineMerge(ST_Union(geom)) FROM route WHERE path_seq > 1)
                                                            -- b			  
                                                            ELSE (SELECT ST_LineMerge(ST_Union(geom)) FROM route) 
                                                            END)
                            END),';
        q3 text := E'' || 'start_edge AS (SELECT geom FROM {db_schema}.{shp_table} WHERE gid = ' || a_edge_id || '),';
        q4 text := E'' || 'start_frac_1 AS (SELECT ST_LineLocatePoint((SELECT geom FROM start_edge),(SELECT {db_schema}.get_node_geom(''' || $1 || ''')))),';
        q5 text := E'' || 'start_frac_2 AS (SELECT CASE WHEN (SELECT source FROM route WHERE path_seq = 1) = ' || a_node_id || '
                            -- 1
                                        THEN ( SELECT CASE WHEN (SELECT edge FROM route WHERE path_seq = 1)=' || a_edge_id || ' 
                                                            -- a
                                                            THEN (SELECT ST_LineLocatePoint((SELECT geom FROM start_edge),
                                                                                            (SELECT the_geom FROM {db_schema}.{shp_table}_vertices_pgr 
                                                                                            WHERE id = (SELECT target FROM route WHERE path_seq = 1))))
                                                            -- b
                                                            ELSE (SELECT ST_LineLocatePoint((SELECT geom FROM start_edge),
                                                                                            (SELECT the_geom FROM {db_schema}.{shp_table}_vertices_pgr 
                                                                                            WHERE id = (SELECT source FROM route WHERE path_seq = 1)))) 
                                                            END)
                            -- 2
                                        ELSE ( SELECT CASE WHEN (SELECT edge FROM route WHERE path_seq = 1)=' || a_edge_id || '
                                                            -- a
                                                            THEN (SELECT ST_LineLocatePoint((SELECT geom FROM start_edge),
                                                                                            (SELECT the_geom FROM {db_schema}.{shp_table}_vertices_pgr 
                                                                                            WHERE id = (SELECT source FROM route WHERE path_seq = 1))))
                                                            -- b			  
                                                            ELSE (SELECT ST_LineLocatePoint((SELECT geom FROM start_edge),
                                                                                            (SELECT the_geom FROM {db_schema}.{shp_table}_vertices_pgr 
                                                                                            WHERE id = (SELECT target FROM route WHERE path_seq = 1)))) 
                                                            END)
                            END),';
        q6 text := E'' || 'start_geom AS (SELECT ST_LineSubstring((SELECT geom FROM start_edge),(SELECT LEAST ((SELECT * FROM start_frac_1),(SELECT * FROM start_frac_2))),(SELECT GREATEST ((SELECT * FROM start_frac_1),(SELECT * FROM start_frac_2)))))';
        q7 text := E'' || 'SELECT ST_LineMerge(ST_Union(ARRAY[(SELECT * FROM start_geom),(SELECT * FROM end_route)]))';

        query_ text:= q1 || q2 || q3 || q4 || q5 || q6 || q7;
        BEGIN 
        --  RAISE NOTICE 'query_: %', query_;
            RETURN QUERY 
            EXECUTE query_ USING $1, $2, $3;
        END
        $BODY$;"""

    get_shortest_path_3ab = f"""CREATE OR REPLACE FUNCTION {db_schema}.get_shortest_path_3ab(
            a_node character varying,
            b_node character varying,
            buffer integer)
            RETURNS TABLE(geom_ geometry) 
            LANGUAGE 'plpgsql'
            COST 100
            VOLATILE PARALLEL UNSAFE
            ROWS 1000

        AS $BODY$
        DECLARE 

        a_node_id int := (SELECT node_id FROM {db_schema}.nodesuk WHERE stop_id = $1);
        b_node_id int := (SELECT node_id FROM {db_schema}.nodesuk WHERE stop_id = $2);

        a_edge_id int := (SELECT {db_schema}.get_nearest_edge_1($1));
        b_edge_id int := (SELECT {db_schema}.get_nearest_edge_1($2));

        q1 text := E'' || 'WITH start_edge AS (SELECT geom FROM {db_schema}.{shp_table} WHERE gid = ' || a_edge_id || '),';
        q2 text := E'' || 'start_frac_1 AS (SELECT ST_LineLocatePoint((SELECT geom FROM start_edge),(SELECT {db_schema}.get_node_geom(''' || $1 || ''')))),';
        q3 text := E'' || 'start_frac_2 AS (SELECT ST_LineLocatePoint((SELECT geom FROM start_edge),(SELECT the_geom FROM {db_schema}.{shp_table}_vertices_pgr WHERE id = ' || a_node_id || '))),';
        q4 text := E'' || 'start_geom AS (SELECT ST_LineSubstring((SELECT geom FROM start_edge),(SELECT LEAST ((SELECT * FROM start_frac_1),(SELECT * FROM start_frac_2))),(SELECT GREATEST ((SELECT * FROM start_frac_1),(SELECT * FROM start_frac_2))))),';

        q5 text := E'' || 'end_edge AS (SELECT geom FROM {db_schema}.{shp_table} WHERE gid = ' || b_edge_id || '),';
        q6 text := E'' || 'end_frac_1 AS (SELECT ST_LineLocatePoint((SELECT geom FROM end_edge),(SELECT {db_schema}.get_node_geom(''' || $2 || ''')))),';
        q7 text := E'' || 'end_frac_2 AS (SELECT ST_LineLocatePoint((SELECT geom FROM end_edge),(SELECT the_geom FROM {db_schema}.{shp_table}_vertices_pgr WHERE id = ' || b_node_id || ' ))),';
        q8 text := E'' || 'end_geom AS (SELECT ST_LineSubstring((SELECT geom FROM end_edge),(SELECT LEAST ((SELECT * FROM end_frac_1),(SELECT * FROM end_frac_2))),(SELECT GREATEST ((SELECT * FROM end_frac_1),(SELECT * FROM end_frac_2)))))';

        q9 text := E'' || 'SELECT ST_LineMerge(ST_Union(ARRAY[(SELECT * FROM start_geom),(SELECT * FROM end_geom)]))';

        query_ text:= q1 || q2 || q3 || q4 || q5 || q6 || q7 || q8 || q9;

        BEGIN 
        -- 	RAISE NOTICE 'query_: %', query_;
            RETURN QUERY 
            EXECUTE query_ USING $1, $2, $3;
        END
        $BODY$;"""

    get_shortest_path_3b = f"""CREATE OR REPLACE FUNCTION {db_schema}.get_shortest_path_3b(
            a_node character varying,
            b_node character varying,
            buffer integer)
            RETURNS TABLE(geom_ geometry) 
            LANGUAGE 'plpgsql'
            COST 100
            VOLATILE PARALLEL UNSAFE
            ROWS 1000

        AS $BODY$
        DECLARE 

        geom_a text := (SELECT ST_AsTEXT(geom) FROM {db_schema}.nodesuk WHERE stop_id = $1);
        geom_b text := (SELECT ST_AsTEXT(geom) FROM {db_schema}.nodesuk WHERE stop_id = $2);

        a_node_id int := (SELECT node_id FROM {db_schema}.nodesuk WHERE stop_id = $1);
        b_node_id int := (SELECT node_id FROM {db_schema}.nodesuk WHERE stop_id = $2);

        b_edge_id int := (SELECT {db_schema}.get_nearest_edge_2($2, b_node_id));

        q1 text :='WITH route AS (
                    SELECT * 
                    FROM {db_schema}.{shp_table} 
                    JOIN(SELECT *
                        FROM pgr_dijkstra(''SELECT gid as id, source, target, st_length(geom) as cost 
                                            FROM {db_schema}.{shp_table} 
                                            WHERE geom && ST_Expand(ST_Envelope(ST_Collect(ST_GeomFromText(''''' || geom_a || '''''), 
                                                                                        ST_GeomFromText(''''' || geom_b ||'''''))
                                                                                ),'||$3||')''' || 
                                            ', ' || a_node_id || 
                                            ', ' || b_node_id || 
                                            ',false
                                        )
                        ) AS route ON {db_schema}.{shp_table}.gid = route.edge),';
        q2 text := E'' || 'start_route AS (SELECT CASE WHEN (SELECT source FROM route WHERE path_seq = (SELECT MAX(path_seq) FROM route)) = ' || b_node_id || '
                            -- 1
                                        THEN ( SELECT CASE WHEN (SELECT edge FROM route WHERE path_seq = (SELECT MAX(path_seq) FROM route))=' || b_edge_id || '
                                                            -- a
                                                            THEN (SELECT ST_LineMerge(ST_Union(geom)) FROM route WHERE path_seq < (SELECT MAX(path_seq) FROM route))
                                                            -- b
                                                            ELSE (SELECT ST_LineMerge(ST_Union(geom)) FROM route) 
                                                            END)
                            -- 2
                                        ELSE ( SELECT CASE WHEN (SELECT edge FROM route WHERE path_seq = (SELECT MAX(path_seq) FROM route))=' || b_edge_id || '
                                                            -- a
                                                            THEN (SELECT ST_LineMerge(ST_Union(geom)) FROM route WHERE path_seq < (SELECT MAX(path_seq) FROM route))
                                                            -- b			  
                                                            ELSE (SELECT ST_LineMerge(ST_Union(geom)) FROM route) 
                                                            END)
                            END),';
        q3 text := E'' || 'end_edge AS (SELECT geom FROM {db_schema}.{shp_table} WHERE gid = (SELECT {db_schema}.get_nearest_edge_2(''' || $2 || ''', ' || b_node_id || '))),';
        q4 text := E'' || 'end_frac_1 AS (SELECT ST_LineLocatePoint((SELECT geom FROM end_edge),(SELECT {db_schema}.get_node_geom(''' || $2 || ''')))),';
        q5 text := E'' || 'end_frac_2 AS (SELECT CASE WHEN (SELECT source FROM route WHERE path_seq = (SELECT MAX(path_seq) FROM route)) = ' || b_node_id || '
                            -- 1
                                        THEN ( SELECT CASE WHEN (SELECT edge FROM route WHERE path_seq = (SELECT MAX(path_seq) FROM route))=' || b_edge_id || ' 
                                                            -- a
                                                            THEN (SELECT ST_LineLocatePoint((SELECT geom FROM end_edge),
                                                                                            (SELECT the_geom FROM {db_schema}.{shp_table}_vertices_pgr 
                                                                                            WHERE id = (SELECT target FROM route WHERE path_seq = (SELECT MAX(path_seq) FROM route)))))
                                                            -- b
                                                            ELSE (SELECT ST_LineLocatePoint((SELECT geom FROM end_edge),
                                                                                            (SELECT the_geom FROM {db_schema}.{shp_table}_vertices_pgr 
                                                                                            WHERE id = (SELECT source FROM route WHERE path_seq = (SELECT MAX(path_seq) FROM route))))) 
                                                            END)
                            -- 2
                                        ELSE ( SELECT CASE WHEN (SELECT edge FROM route WHERE path_seq = (SELECT MAX(path_seq) FROM route))=' || b_edge_id || '
                                                            -- a
                                                            THEN (SELECT ST_LineLocatePoint((SELECT geom FROM end_edge),
                                                                                            (SELECT the_geom FROM {db_schema}.{shp_table}_vertices_pgr 
                                                                                            WHERE id = (SELECT source FROM route WHERE path_seq = (SELECT MAX(path_seq) FROM route)))))
                                                            -- b			  
                                                            ELSE (SELECT ST_LineLocatePoint((SELECT geom FROM end_edge),
                                                                                            (SELECT the_geom FROM {db_schema}.{shp_table}_vertices_pgr 
                                                                                            WHERE id = (SELECT target FROM route WHERE path_seq = (SELECT MAX(path_seq) FROM route))))) 
                                                            END)
                            END),';
        q6 text := E'' || 'end_geom AS (SELECT ST_LineSubstring((SELECT geom FROM end_edge),
                                        (SELECT    LEAST ((SELECT * FROM end_frac_1),(SELECT * FROM end_frac_2))),
                                        (SELECT GREATEST ((SELECT * FROM end_frac_1),(SELECT * FROM end_frac_2)))))';
        q7 text := E'' || 'SELECT ST_LineMerge(ST_Union(ARRAY[(SELECT * FROM start_route),(SELECT * FROM end_geom)]))';

        query_ text:= q1 || q2 || q3 || q4 || q5 || q6 || q7;

        BEGIN 
        --	RAISE NOTICE 'query_: %', query_;
            RETURN QUERY 
            EXECUTE query_ USING $1, $2, $3;
        END
        $BODY$;"""

    get_shortest_path_4a = f"""CREATE OR REPLACE FUNCTION {db_schema}.get_shortest_path_4a(
            a_node character varying,
            b_node character varying,
            buffer integer)
            RETURNS TABLE(geom_ geometry) 
            LANGUAGE 'plpgsql'
            COST 100
            VOLATILE PARALLEL UNSAFE
            ROWS 1000

        AS $BODY$
        DECLARE 

        geom_a text := (SELECT ST_AsTEXT(geom) FROM {db_schema}.nodesuk WHERE stop_id = $1);
        geom_b text := (SELECT ST_AsTEXT(geom) FROM {db_schema}.nodesuk WHERE stop_id = $2);

        a_node_id int := (SELECT node_id FROM {db_schema}.nodesuk WHERE stop_id = $1);
        b_node_id int := (SELECT node_id FROM {db_schema}.nodesuk WHERE stop_id = $2);

        a_edge_id int := (SELECT {db_schema}.get_nearest_edge_2($1, a_node_id));q1 text :='WITH route AS (
                        SELECT * 
                        FROM {db_schema}.{shp_table} 
                        JOIN(SELECT *
                            FROM pgr_dijkstra(''SELECT gid as id, source, target, st_length(geom) as cost 
                                                FROM {db_schema}.{shp_table} 
                                                WHERE geom && ST_Expand(ST_Envelope(ST_Collect(ST_GeomFromText(''''' || geom_a || '''''), 
                                                                                                ST_GeomFromText(''''' || geom_b ||'''''))
                                                                                    ),'||$3||')''' || 
                                                ', ' || a_node_id || 
                                                ', ' || b_node_id || 
                                                ',false
                                            )
                            ) AS route ON {db_schema}.{shp_table}.gid = route.edge),';
        q2 text := E'' || 'circ_edge AS (SELECT geom FROM {db_schema}.{shp_table} WHERE gid = ' || a_edge_id || '),';
        q3 text := E'' || 'circ_frac_1 AS (SELECT ST_LineLocatePoint((SELECT geom FROM circ_edge),(SELECT {db_schema}.get_node_geom(''' || $1 || ''')))),';
        q4 text := E'' || 'circ_frac_2 AS (SELECT ST_LineLocatePoint((SELECT geom FROM circ_edge),(SELECT the_geom FROM {db_schema}.{shp_table}_vertices_pgr WHERE id = ' || a_node_id || '))),';
        q5 text := E'' || 'circ_start AS (SELECT ST_LineSubstring((SELECT geom FROM circ_edge),(SELECT LEAST    ((SELECT * FROM circ_frac_1),(SELECT * FROM circ_frac_2))),(SELECT GREATEST ((SELECT * FROM circ_frac_1),(SELECT * FROM circ_frac_2)))))';
        q6 text := E'' || 'SELECT ST_LineMerge(ST_Union(ARRAY[(SELECT * FROM circ_start),(SELECT ST_Union(geom) FROM route)]))';
        query_ text:= q1 || q2 || q3 || q4 || q5 || q6;
        BEGIN 
            -- RAISE NOTICE 'query_: %', query_;
            RETURN QUERY 
            EXECUTE query_ USING $1, $2, $3;
        END
        $BODY$;"""

    get_shortest_path_4b = f"""CREATE OR REPLACE FUNCTION {db_schema}.get_shortest_path_4b(
            a_node character varying,
            b_node character varying,
            buffer integer)
            RETURNS TABLE(geom_ geometry) 
            LANGUAGE 'plpgsql'
            COST 100
            VOLATILE PARALLEL UNSAFE
            ROWS 1000

        AS $BODY$
        DECLARE 

        geom_a text := (SELECT ST_AsTEXT(geom) FROM {db_schema}.nodesuk WHERE stop_id = $1);
        geom_b text := (SELECT ST_AsTEXT(geom) FROM {db_schema}.nodesuk WHERE stop_id = $2);

        a_node_id int := (SELECT node_id FROM {db_schema}.nodesuk WHERE stop_id = $1);
        b_node_id int := (SELECT node_id FROM {db_schema}.nodesuk WHERE stop_id = $2);

        b_edge_id int := (SELECT {db_schema}.get_nearest_edge_2($2, b_node_id));

        q1 text :='WITH route AS (
            SELECT * 
            FROM {db_schema}.{shp_table} 
            JOIN(SELECT *
                FROM pgr_dijkstra(''SELECT gid as id, source, target, st_length(geom) as cost 
                                    FROM {db_schema}.{shp_table} 
                                    WHERE geom && ST_Expand(ST_Envelope(ST_Collect(ST_GeomFromText(''''' || geom_a || '''''), 
                                                                                    ST_GeomFromText(''''' || geom_b ||'''''))
                                                                        ),'||$3||')''' || 
                                    ', ' || a_node_id || 
                                    ', ' || b_node_id || 
                                    ',false
                                )
                ) AS route ON {db_schema}.{shp_table}.gid = route.edge),';

        q2 text := E'\n' || 'circ_edge AS (SELECT geom FROM {db_schema}.{shp_table} WHERE gid = ' || b_edge_id || '),';
        
        q3 text := E'\n' || 'circ_frac_1 AS (SELECT ST_LineLocatePoint((SELECT geom FROM circ_edge),
                                                                        (SELECT {db_schema}.get_node_geom(''' || $2 || ''')))),';
        
        q4 text := E'\n' || 'circ_frac_2 AS (SELECT ST_LineLocatePoint((SELECT geom FROM circ_edge),
                                                                        (SELECT the_geom FROM {db_schema}.{shp_table}_vertices_pgr 
                                                                        WHERE id = ' || b_node_id || '))),';
        
        q5 text := E'\n' || 'circ_end AS (SELECT ST_LineSubstring(
                                                                    (SELECT geom FROM circ_edge),
                                                                    (SELECT LEAST    ((SELECT * FROM circ_frac_1),(SELECT * FROM circ_frac_2))),
                                                                    (SELECT GREATEST ((SELECT * FROM circ_frac_1),(SELECT * FROM circ_frac_2)))))';
        
        q6 text := E'\n' || 'SELECT ST_LineMerge(ST_Union(ARRAY[(SELECT ST_Union(geom) FROM route),(SELECT * FROM circ_end)]))';
        
        query_ text:= q1 || q2 || q3 || q4 || q5 || q6;
            
        BEGIN 
            -- RAISE NOTICE 'query_: %', query_;
            RETURN QUERY 
            EXECUTE query_ USING $1, $2, $3;
        END
        $BODY$;"""

    conn2 = connect(db_name, db_user, db_password, db_server, db_port)
    cursor = conn2.cursor()
    
    pg_functions = [
                    ab_routes_calc_0,
                    ab_routes_calc_1,
                    ab_routes_calc_2,
                    ab_routes_calc_3a,
                    ab_routes_calc_3b,
                    ab_routes_calc_3ab,
                    ab_routes_calc_4a,
                    ab_routes_calc_4b,
                    ab_routes_table,
                    fraction_calc,
                    get_edge_by_node,
                    get_edge_by_stopid,
                    get_nearest_edge_1,
                    get_nearest_edge_2,
                    get_nearest_node_1,
                    get_nearest_node_2,
                    get_node_geom,                            
                    get_point_by_node,
                    get_shortest_path_1,
                    get_shortest_path_2,
                    get_shortest_path_3a,
                    get_shortest_path_3b,
                    get_shortest_path_3ab,
                    get_shortest_path_4a,
                    get_shortest_path_4b
                    ]

    for func in pg_functions:
        cursor.execute(func)
        conn2.commit()
    
    print(f"{Fore.GREEN}Done!")
    cursor.close()

if __name__ == "__main__":
    db_connect()
    connect()