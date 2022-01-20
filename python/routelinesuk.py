# ==============================================================================
# RoutelinesUK
# ------------------------------------------------------------------------------

#     version:  1.0
#     date:     2021-June-06
#     author:   Nebojša Pešić, "NMItools" (nmitools@gmail.com)

# ==============================================================================

import os
import sys
import argparse

from colorama import init
from colorama import Fore, Back
# from sqlalchemy.sql.expression import table
# from termcolor import colored

from setup import db_server, db_port, db_def, db_name, db_schema, db_user, db_password, pts_table, shp_table, buffer 
from setup import version, dir_prj, dir_cif, cif_file, dir_shp, shp_file, shp_out

from setup import config_write, config_print, timestamp, msg

from cif import read_cif, GetPtStops, GetRouteDataToSQL, ptstops_update, rl1_update, mt_update, CreateFreq, ABsorter, abnodesuk

from rlshp import shp_import, topo_sa, topo_pg, routelines_shp_export

from rldb import db_connect, connect, db_setup, db_func

from rlroute import nodesuk, abnodesuk_update, f_ab_routes_calc, f_ab_routes_table, routes_geom, routelines_incomplete, routelines_final

def db(args):
    
    db_setup(db_server, db_port, db_name, db_def, db_user, db_password)
    db_func(db_server, db_port, db_name, db_schema, db_user, db_password, shp_table)

def sp(args):
    
    db_func(db_server, db_port, db_name, db_schema, db_user, db_password, shp_table)
    
def config(args):
    
    config_print()

    ask_cont = input(f"{Fore.YELLOW}Do you want to change the configuration parameters? (y/n)")
    if ask_cont == "y":
        config_write('setup.ini')
  
    return

def cif(args):

    print(95*f"{Fore.GREEN}=")
    print(f"{Fore.LIGHTCYAN_EX}[CIF file processing and importing into a database]")
    print(95*f"{Fore.GREEN}-")

    region ="UK"

    t1, tp1 = timestamp()[0], timestamp()[1]
    print(f"{Fore.GREEN}Preparation of CIF file [{dir_cif+cif_file}] started at {tp1}")
    
    cif_df = read_cif(dir_cif+cif_file)
    t2, tp2 = timestamp()[0], timestamp()[1]
    print(f"{Fore.GREEN}Finished in {round((t2 - t1).total_seconds(),1)} sec.")
    print("---")
    
    GetPtStops(conn, cif_df, db_schema, pts_table)
    t3, tp3 = timestamp()[0], timestamp()[1]
    print(f"{Fore.GREEN}Finished in {round((t3 - t2).total_seconds(),1)} sec.")
    print("---")

    GetRouteDataToSQL(conn, conn2, db_schema, cif_df, region)
    t4, tp4 = timestamp()[0], timestamp()[1]
    print(f"{Fore.GREEN}Finished in {round((t4 - t3).total_seconds()/60,1)} min.")
    print("---")

    ptstops_update(conn, db_schema, pts_table) 
    t5, tp5 = timestamp()[0], timestamp()[1]
    print(f"{Fore.GREEN}Finished in {round((t5 - t4).total_seconds(),1)} sec.")
    print("---")

    rl1_update(conn, db_schema, pts_table)
    t6, tp6 = timestamp()[0], timestamp()[1]
    print(f"{Fore.GREEN}Finished in {round((t6 - t5).total_seconds()/60,1)} min.")
    print("---")

    mt_update(conn, db_schema, pts_table)
    t7, tp7 = timestamp()[0], timestamp()[1]
    print(f"{Fore.GREEN}Finished in {round((t7 - t6).total_seconds()/60,1)} min.")
    print("---")

    CreateFreq(conn, db_schema)
    t8, tp8 = timestamp()[0], timestamp()[1]
    print(f"{Fore.GREEN}Finished in {round((t8 - t7).total_seconds()/60,1)} min.")
    print("---")

    ABsorter(conn, db_schema, pts_table)
    t9, tp9 = timestamp()[0], timestamp()[1]    
    print(f"{Fore.GREEN}Finished in {round((t9 - t8).total_seconds(),1)} sec.")
    print("---")
    
    abnodesuk(conn, db_schema)
    t10, tp10 = timestamp()[0], timestamp()[1]    
    print(f"{Fore.GREEN}Finished in {round((t10 - t9).total_seconds(),1)} sec.")
    print("---")    

    print(f"{Fore.LIGHTCYAN_EX}[Creation of 'RLFreqUK']")
    conn.execute(f"DROP TABLE IF EXISTS {db_schema}.rlfrequk CASCADE")
    conn.execute(f"CREATE TABLE {db_schema}.rlfrequk AS SELECT * FROM {db_schema}.rlfreq")
    conn.execute(f"ALTER TABLE {db_schema}.rlfrequk ADD CONSTRAINT rlfrequk_pkey PRIMARY KEY (id)")
    conn.execute(f"CREATE INDEX bm_routeid_idx ON {db_schema}.rlfrequk USING btree (bm_routeid text_pattern_ops ASC NULLS LAST) TABLESPACE pg_default")
    t10, tp10 = timestamp()[0], timestamp()[1]    
    print(f"Finished in {round((t10 - t9).total_seconds(),1)} sec.")
    print("---")    

    print(f"{Fore.LIGHTCYAN_EX}[Creation of 'RoutelinesUK']")
    conn.execute(f"DROP TABLE IF EXISTS {db_schema}.routelinesuk CASCADE")
    conn.execute(f"CREATE TABLE {db_schema}.routelinesuk AS SELECT bmrouteid, anodestopid, bnodestopid FROM {db_schema}.routelines1 GROUP BY bmrouteid, anodestopid, bnodestopid")
    conn.execute(f"CREATE INDEX bmrouteid_idx ON {db_schema}.routelinesuk USING btree (bmrouteid text_pattern_ops ASC NULLS LAST) TABLESPACE pg_default")
    conn.execute(f"CREATE INDEX anodestopid_idx ON {db_schema}.routelinesuk USING btree (anodestopid text_pattern_ops ASC NULLS LAST) TABLESPACE pg_default")
    conn.execute(f"CREATE INDEX bnodestopid_idx ON {db_schema}.routelinesuk USING btree (bnodestopid text_pattern_ops ASC NULLS LAST) TABLESPACE pg_default")
    conn.execute(f"SELECT DISTINCT bmrouteid FROM {db_schema}.routelinesuk ORDER BY bmrouteid;")
    
    query = conn.execute(f"SELECT COUNT(DISTINCT bmrouteid) FROM {db_schema}.routelinesuk").fetchone()[0]
    print(f"{Fore.LIGHTGREEN_EX}Number of unique route lines generated: {query}")

    t11, tp11 = timestamp()[0], timestamp()[1]    
    print(f"{Fore.GREEN}Finished in {round((t11 - t10).total_seconds(),1)} sec.")
    print(f"{Fore.GREEN}---")    

    print(f"{Fore.LIGHTCYAN_EX}Processing CIF file [{dir_cif+cif_file}] completed in {round((t11 - t1).total_seconds()/60,1)} minutes.") 
    print(95*"=")

def shp(args):
    shp_import(conn, dir_shp+shp_file, db_schema, shp_table)
    # topo_sa(conn, db_schema, shp_table)
    topo_pg(conn2, db_schema, shp_table)

def route(args):
    nodesuk(conn2, db_schema, shp_table, dir_cif, cif_file)
    abnodesuk_update(conn2, db_schema, buffer)
    f_ab_routes_table(conn2, db_schema, 'ab_routes')
    f_ab_routes_calc(conn2, db_schema)
    routes_geom(conn2, db_schema)
    routelines_incomplete(conn2, db_schema)
    routelines_final(conn2, db_schema, dir_shp, shp_out)
    routelines_shp_export(conn, db_schema, dir_shp, shp_out)

if __name__ == "__main__":

    # starting the app and printing the version and the name

    os.system('cls')
    # os.system('COLOR 73')
    # $Host.UI.SupportsVirtualTerminal
    init(autoreset=True)
    
    print(95*f"{Fore.CYAN}=")
    print(f"{Back.LIGHTBLACK_EX}{Fore.LIGHTCYAN_EX}RoutelinesUK v{version}")

    conn = db_connect(db_server, db_port, db_name, db_user, db_password)

    try:
        conn2 = connect(db_name, db_user, db_password, db_server, db_port)
    except Exception as error:
        print(f"{Fore.LIGHTRED_EX}Could not connect to '{db_name}' database on server '{db_server}'!")
        print(f"{Fore.LIGHTRED_EX} - {error}")
        print(f"{Fore.LIGHTRED_EX}Please check application setup (config, db).")
        sys.exit(1)
    else:
        print(f"{Fore.LIGHTGREEN_EX}Successfully connected to database '{db_name}' on server '{db_server}' !")
    finally:
        if len(sys.argv) >= 2:
            # top-level parser
            parser = argparse.ArgumentParser(prog='RouteLinesUK',
                                            description="A command-line tool in Python for calculating the shortest path between BUS stop sequences in the UK."
                                            )
            subparsers = parser.add_subparsers()

            # --------------------------------------------------------------------------------------------------------------------------------------------------
            # 'config' parser

            par_config = subparsers.add_parser('config',
                                            description="Project configuration and INI file creation."
                                            )
            par_config.set_defaults(func=config)

            # --------------------------------------------------------------------------------------------------------------------------------------------------
            # 'db' parser

            par_db = subparsers.add_parser('db',
                                            description="Create a fresh project database."
                                            )
            par_db.set_defaults(func=db)
            
            # --------------------------------------------------------------------------------------------------------------------------------------------------
            # 'sp' parser

            par_sp = subparsers.add_parser('sp',
                                            description="Update functions (stored procedures) in project database."
                                            )
            par_sp.set_defaults(func=sp)

            # --------------------------------------------------------------------------------------------------------------------------------------------------
            # 'cif' parser

            par_cif = subparsers.add_parser('cif',
                                            description="Processing of CIF file and extracting routing data into a PostgreSQL database."
                                            )
            par_cif.set_defaults(func=cif)
            
            # --------------------------------------------------------------------
            # 'SHP' parser

            par_shp = subparsers.add_parser('shp',
                                            description="Imports ESRI® Shapefile into a PostgreSQL database."
                                            )
            par_shp.set_defaults(func=shp)
            
            # --------------------------------------------------------------------            
            # 'route' parser

            par_shp = subparsers.add_parser('route',
                                            description="Prepare imported data for routing."
                                            )
            par_shp.set_defaults(func=route)
            
            # --------------------------------------------------------------------                   
            args = parser.parse_args()
            args.func(args)

        else:
            print(95*f"{Fore.GREEN}-")
            # msg('title',f"{Fore.GREEN}For help type 'python routelinesuk.py -h'")
            msg('info',f"{Fore.LIGHTCYAN_EX}For help type 'python routelinesuk.py -h'")
            # msg('error',f"{Fore.GREEN}For help type 'python routelinesuk.py -h'")
            # print(f"{Fore.LIGHTCYAN_EX}For help type 'python routelinesuk.py -h'") 
                                       
