import configparser

config = configparser.ConfigParser()

config['database'] = {'db_server': 'localhost'
                     ,'db_port': '5434'
                     ,'db_name': 'routelinesuk'
                     ,'db_schema': 'rl'
                     ,'db_user': 'postgres'
                     ,'db_password': 'softdesk'
                     }
 
config['project'] = {'version': '1.0'
                    ,'dir_prj': 'C:/Routelines/'
                    ,'dir_cif': 'C:/Routelines/data/'
                    ,'cif_file': 'Bus_1.cif'
                    ,'pts_table': 'PTStops'
                    }

with open('setup.ini', 'w') as configfile:
    config.write(configfile)