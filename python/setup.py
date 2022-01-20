# =============================================================
# global variables definitions
# =============================================================

import click
import configparser

from colorama import init
init(autoreset=True)
from colorama import Fore

# from termcolor import colored

import string
import random
from random import randint
from datetime import datetime

import configparser

config = configparser.ConfigParser()
config.read('setup.ini')

db_server = config.get('database', 'db_server')
db_port = config.getint('database', 'db_port')
db_def = config.get('database', 'db_def')
db_name = config.get('database', 'db_name')
db_schema = config.get('database', 'db_schema')
db_user = config.get('database', 'db_user')
db_password = config.get('database', 'db_password')
pts_table = config.get('database', 'pts_table')
shp_table = config.get('database', 'shp_table')
buffer = config.get('database', 'buffer')

# read values from a section PROJECT
version = config.get('project', 'version')
dir_prj = config.get('project', 'dir_prj')
dir_cif = config.get('project', 'dir_cif')
dir_shp = config.get('project', 'dir_shp')
cif_file = config.get('project', 'cif_file')
shp_file = config.get('project', 'shp_file')
shp_out = config.get('project', 'shp_out')

def random_char(y):
    char = ''.join(random.choice(string.ascii_letters) for x in range(y))
    num = randint(0, 123456)
    return char+str(num)        

idx_no = random_char(5)

def timestamp():
    time = datetime.now()
    time_hms = time.strftime("%H:%M:%S")
    return [time, time_hms]

def config_print():

    print(95*f"{Fore.GREEN}-")
    print(f"{Fore.GREEN}Current configuration:")
    print(95*f"{Fore.GREEN}-")
    print("Project data folder   : ", dir_prj)
    print("CIF data folder       : ", dir_cif)
    print("Shapefile data folder : ", dir_shp)
    print("CIF file              : ", cif_file)
    print("SHP input file        : ", shp_file)
    print("SHP output file       : ", shp_out)
    print(95*"-")
    print("db server   : ", db_server)
    print("db port     : ", db_port)
    print("db default  : ", db_def)
    print("database    : ", db_name)
    print("schema      : ", db_schema)
    print("user        : ", db_user)
    print("password    : ", db_password)
    print("PTStops tablename     : ", pts_table)
    print("SHP tablename         : ", shp_table)
    print("Buffer size in m      : ", buffer)
    print(95*"-")    

def config_write(ini_filename):
    config = configparser.ConfigParser()
    config.read(ini_filename)

    print("Set database parameters (ENTER for default values):")
    print(23*"-")
    
    db_server = click.prompt("db_server: ", type=str, default=config.get('database', 'db_server'))
    config.set('database', 'db_server', db_server)

    db_port = click.prompt("db port: ", type=str, default=config.getint('database', 'db_port'))
    config.set('database', 'db_port', str(db_port))

    db_def = click.prompt("db def: ", type=str, default=config.get('database', 'db_def'))
    config.set('database', 'db_def', db_def)

    db_name = click.prompt("db name: ", type=str, default=config.get('database', 'db_name'))
    config.set('database', 'db_name', db_name)

    db_schema = click.prompt("db schema: ", type=str, default=config.get('database', 'db_schema'))
    config.set('database', 'db_schema', db_schema)

    db_user = click.prompt("db user: ", type=str, default=config.get('database', 'db_user'))
    config.set('database', 'db_user', db_user)

    db_password = click.prompt("db password: ", type=str, default=config.get('database', 'db_password'))
    config.set('database', 'db_password', db_password)

    pts_table = click.prompt("pts_table: ", type=str, default=config.get('database', 'pts_table'))
    config.set('database', 'pts_table', pts_table)

    shp_table = click.prompt("shp_table: ", type=str, default=config.get('database', 'shp_table'))
    config.set('database', 'shp_table', shp_table)

    buffer = click.prompt("buffer: ", type=str, default=config.get('database', 'buffer'))
    config.set('database', 'buffer', buffer)

    print("Set project file management parameters:")
    print(38*"-")

    dir_prj = click.prompt("Project data main folder: ", type=str, default=config.get('project', 'dir_prj'))
    config.set('project', 'dir_prj', dir_prj)

    dir_cif = click.prompt("CIF data folder: ", type=str, default=config.get('project', 'dir_cif'))
    config.set('project', 'dir_cif', dir_cif)

    dir_shp = click.prompt("SHP data folder: ", type=str, default=config.get('project', 'dir_shp'))
    config.set('project', 'dir_shp', dir_shp)

    cif_file = click.prompt("CIF filename: ", type=str, default=config.get('project', 'cif_file'))
    config.set('project', 'cif_file', cif_file)

    shp_file = click.prompt("SHP input filename: ", type=str, default=config.get('project', 'shp_file'))
    config.set('project', 'shp_file', shp_file)

    shp_out = click.prompt("SHP output filename: ", type=str, default=config.get('project', 'shp_out'))
    config.set('project', 'shp_out', shp_out)

    try:
        with open(ini_filename, 'w') as configfile:
            config.write(configfile)
    except IOError:
        print(f"Unable to create {ini_filename} file!")

def msg(type,text):
    if type == 'title':
        print(95*f"{Fore.GREEN}=")
        print(f"{Fore.GREEN}[{text}]\n")
    elif type == 'info':
         print(f"{Fore.LIGHTGREEN_EX}{text}\n")
    elif type == 'error':
         print(f"{Fore.LIGHTRED_EX}{text}\n")



