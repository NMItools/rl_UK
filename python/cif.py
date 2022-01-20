import os
import sys
import math
import mmap

import io
import psycopg2

from colorama import init
init(autoreset=True)
from colorama import Fore

import pandas as pd
from tqdm import tqdm

from setup import idx_no

# ===========================================================================================================
# 1. CIF data extraction functions
# ===========================================================================================================

def mapcount(filename):
    print("Counting number of lines...")
    f = open(filename, "r+")
    buf = mmap.mmap(f.fileno(), 0)
    lines = 0
    readline = buf.readline
    while readline():
        lines += 1
    print(f'{lines} lines in the file!')
    return lines


# Reading CIF file


def read_cif(cif_file):
    print(95*"-")
    print(f'CIF file size is {round(os.stat(cif_file).st_size / (1024 * 1024), 2)} MB')   
    count_lines = mapcount(cif_file)
    chunk_size=1000000
    chunks = []
    loops = math.ceil(count_lines/chunk_size)
    i=0
    with tqdm(total = loops, file = sys.stdout, ncols=95) as pbar:
        reader = pd.read_csv(cif_file, names=['CODE'], header=None, sep='!', iterator=True)
        while i <= loops:
            try:
                i+=1
                chunk = reader.get_chunk(chunk_size)
                chunks.append(chunk)
                pbar.set_description('Reading CIF data into memory ')
                pbar.update(1)
            except StopIteration:
                loop = False
                cif_data = pd.concat(chunks, ignore_index=True)
                pbar.update(1)
    pbar.close()
    print("CIF data imported!")
    return cif_data

def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))

def insert_with_progress(conn, df, db_schema, tablename):
    chunksize = int(len(df) / 10) # 10%
    with tqdm(total=len(df), ncols=95) as pbar:
        for i, cdf in enumerate(chunker(df, chunksize)):
            cdf.to_sql(tablename, conn, schema=db_schema, if_exists='append', index=False, method='multi')
            pbar.update(chunksize)
            pbar.set_description('Inserting to database...')
    print("Done!")

def GetPtStops(conn, cif_data, db_schema, tablename):
    print("Extracting QB nodes in progress...")
    QB = cif_data['CODE'].str.extract('(^QB.*)').dropna()
    QB.columns = ['CODE']
    print("QB nodes extracted:", len(QB))
    COLUMN_NAMES = ['naptanid','xcoord','ycoord']
    PTStops = pd.DataFrame(columns=COLUMN_NAMES)
    PTStops['naptanid'] = QB['CODE'].str.slice(start=3, stop=15).str.rstrip()
    PTStops['xcoord'] = QB['CODE'].str.slice(start=15, stop=21)
    PTStops['ycoord'] = QB['CODE'].str.slice(start=23, stop=29)
    # convert columns "xcoord" and "ycoord" to numeric
    PTStops[["xcoord", "ycoord"]] = PTStops[["xcoord", "ycoord"]].apply(pd.to_numeric, downcast='integer')
    conn.execute(f"DROP TABLE IF EXISTS {db_schema}.{tablename}")
    insert_with_progress(conn, PTStops, db_schema, tablename)
    conn.execute(f"CREATE INDEX {idx_no}_ptstops_idx ON {db_schema}.{tablename} USING btree (naptanid text_pattern_ops ASC NULLS LAST) TABLESPACE pg_default")
    return PTStops

def copy_from_stringio(conn, df, table, cols):

    """ Here we are going save the dataframe in memory and use copy_from() to copy it to the table """

    # save dataframe to an in memory buffer

    buffer = io.StringIO()
    df.to_csv(buffer, index_label='id', header=False)
    buffer.seek(0)
    
    cursor = conn.cursor()
    try:
        cursor.copy_from(buffer, table, sep=",", columns=cols )
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print(f"Uploading '{table}' to PostgreSQL done!")
    cursor.close()

def copy_from_stringio_progress(conn, df, table, cols):

    """ Here we are going save the dataframe in memory and use copy_from() to copy it to the db table """
    
    # save dataframe to an in memory buffer
    
    buffer = io.StringIO()
    df.to_csv(buffer, index_label='id', header=False)
    buffer.seek(0)

    cursor = conn.cursor()

    count_lines=df.shape[0]
    chunk_size=500000
    chunks = []
    loops = math.ceil(count_lines/chunk_size)
    i=0

    with tqdm(total = loops, file = sys.stdout, ncols=95) as pbar:
        while i <= loops:
            try:
                i+=1
                reader = cursor.copy_from(buffer, table, sep=",", columns=cols )
                conn.commit()
                pbar.set_description(f"Uploading '{table}' to PostgreSQL ...")
                pbar.update(1)
            except (Exception, psycopg2.DatabaseError) as error:
                print(Fore.LIGHTRED_EX + "\n Error: %s" % error)
                conn.rollback()
                cursor.close()
                return 1
    pbar.close()
    print(f"Uploading '{table}' to PostgreSQL done!")
    cursor.close()


# Routes extraction


def GetRouteDataToSQL(conn, conn2, db_schema, cif_df, region):
    
# temp list for RouteLines1 columns

    l_Region = []
    l_BMRouteID_1 = []
    l_BM_StartStopID_1 = []
    l_AnodeStopID = []
    l_BnodeStopID = []
    l_AnodeXcoord = []
    l_AnodeYcoord = []
    l_BnodeXcoord = []
    l_BnodeYcoord = []
    # l_id_1 = []

# temp list for MainTable columns

    l_OperatorCode = []
    l_ServiceNum = []
    l_BM_RouteID_2 = []
    l_BM_StartStopID_2 = []
    l_Direction = []
    l_StopID = []
    l_DeptTime = []
    l_Seq = []
    l_Mon = []
    l_Tue = []
    l_Wed = []
    l_Thu = []
    l_Fri = []
    l_Sat = []
    l_Sun = []
    l_TotalWeekly = []
    # l_id_2 = []

# routelines1 to PostgreSQL

    conn.execute(f"DROP TABLE IF EXISTS {db_schema}.routelines1_tmp")
    conn.execute('''CREATE TABLE ''' + db_schema + '''.routelines1_tmp ( id integer PRIMARY KEY,
                                                                    region text,
                                                                    bmrouteid text,
                                                                    bm_startstopid text,
                                                                    anodestopid text,
                                                                    bnodestopid text,
                                                                    anodexcoord text,
                                                                    anodeycoord text,
                                                                    bnodexcoord text,
                                                                    bnodeycoord text) TABLESPACE pg_default'''
                                                                    )
    conn.execute(f"ALTER TABLE {db_schema}.routelines1_tmp OWNER to postgres")

# maintable to PostgreSQL

    conn.execute(f"DROP TABLE IF EXISTS {db_schema}.maintable_tmp")
    conn.execute('''CREATE TABLE ''' + db_schema + '''.maintable_tmp( id integer PRIMARY KEY,
                                                                    operatorcode text,
                                                                    servicenum text,
                                                                    bm_routeid text,
                                                                    bm_startstopid text,
                                                                    direction text,
                                                                    stopid text,
                                                                    depttime integer,
                                                                    seq integer,
                                                                    mon integer,
                                                                    tue integer,
                                                                    wed integer,
                                                                    thur integer,
                                                                    fri integer,
                                                                    sat integer,
                                                                    sun integer,
                                                                    totalweekly integer) TABLESPACE pg_default'''
                                                                    )
    conn.execute(f"ALTER TABLE {db_schema}.maintable_tmp OWNER to postgres")

# QS (QO,QI,QT) nodes extraction

    print("Extracting QS (QO,QI,QT) nodes in progress...")
    
    BankHolOnly = "" 

    for x in cif_df['CODE']:
        if x[0:2]=='QS':
    #         -----------------------
    #         QS
    #         -----------------------
            SeqNo = 0
            AnodeStr = "XXXX"
            RouteIDFull = x[38:65]
    #         print("1-",RouteIDFull)
            RouteIDV = RouteIDFull[:4].rstrip()
    #         print("2-",RouteIDV)
            RouteIDFull = x[37:65]
    #         print("3-",RouteIDFull)        
            BankHolOnly = RouteIDFull[0]
    #         print("4-",BankHolOnly)
            if BankHolOnly != "":
                pass
            RouteIDFull =x[3:]
    #         print("5-"+RouteIDFull)                
            RouteOpV = RouteIDFull[:4].rstrip()
    #         print("6-"+RouteOpV)
            ServiceIDv = RouteIDV
            RouteIDFull = x[64:]
    #         print("7-"+RouteIDFull)        
            DirectionV = RouteIDFull[0]
    #       RouteIDV = region + "_" + RouteOpV + "_" +  RouteIDV + "_" + DirectionV
            RouteIDV = RouteOpV + "_" +  RouteIDV + "_" + DirectionV
    #         print("8-"+RouteIDV)
    #         RouteIDFull = x[29:]
            MonV = int(x[29:][0])
    #         print("MonV =", MonV)
            TueV = int(x[30:][0])
    #         print("TueV =", TueV)
            WedV = int(x[31:][0])
    #         print("WedV =", WedV)
            ThuV = int(x[32:][0])
    #         print("ThuV =", ThuV)
            FriV = int(x[33:][0])
    #         print("FriV =", FriV)
            SatV = int(x[34:][0])
    #         print("SatV =", SatV)
            SunV = int(x[35:][0])
    #         print("SunV =", SunV)
    #         print("------------")
        if BankHolOnly == "B":
            BankLoop = BankLoop + 1
            if BankLoop == 1:
                print("WARNING: Bank holiday services exist!")
        else:
            if x[0:2] in ('QO','QI','QT'):
    #             print(x[0:2])
    #             -----------------------
    #             QO / QI / QT
    #             -----------------------
                RouteIDFull = x[2:]
    #             print("9 -", RouteIDFull)
                RSStopID = RouteIDFull[:12].rstrip()
    #             print("10-", RSStopID)
                StopArea = RouteIDFull[:4].rstrip()
                BnodeStr = RSStopID
    #             print("StopArea -",StopArea)
    #             print("BnodeStr -",BnodeStr)
                RouteIDFull = x[14:]
    #             print("11-", RouteIDFull)
                TimeNum = RouteIDFull[:4]
    #             print("12-", TimeNum)
                SeqNo = SeqNo + 1
                if SeqNo == 1:
                    StartStopID = RSStopID
                if SeqNo > 1:
                    # print("RouteLines1:")
                    l_Region.append(region)
                    l_BMRouteID_1.append(RouteIDV)
                    l_BM_StartStopID_1.append(StartStopID)
                    l_AnodeStopID.append(AnodeStr)
                    l_BnodeStopID.append(BnodeStr)
                    l_AnodeXcoord.append(0)
                    l_AnodeYcoord.append(0)
                    l_BnodeXcoord.append(0)
                    l_BnodeYcoord.append(0)
                    # l_id_1.append(SeqNo)
                AnodeStr = RSStopID
                if int(TimeNum) >= 0 and int(TimeNum) < 2400:
                    # print("MainTable:")
                    l_OperatorCode.append(RouteOpV)
                    l_ServiceNum.append(ServiceIDv)
                    l_BM_RouteID_2.append(RouteIDV)
                    l_BM_StartStopID_2.append(StartStopID)
                    l_Direction.append(DirectionV)
                    l_StopID.append(RSStopID)
                    l_DeptTime.append(TimeNum)
                    l_Seq.append(SeqNo)
                    l_Mon.append(MonV)
                    l_Tue.append(TueV)
                    l_Wed.append(WedV)
                    l_Thu.append(ThuV)
                    l_Fri.append(FriV)
                    l_Sat.append(SatV)
                    l_Sun.append(SunV)
                    l_TotalWeekly.append(0) 
                    # l_id_2.append(SeqNo)


# Pandas dataframe creations

    # - RouteLines1
    print("Loading RouteLines1 to Pandas dataframe...")
    
    RouteLines1 = pd.DataFrame(
        {'region':l_Region,
         'bmrouteid':l_BMRouteID_1,
         'bm_startstopid':l_BM_StartStopID_1,
         'anodestopid':l_AnodeStopID,
         'bnodestopid':l_BnodeStopID,
         'anodexcoord':l_AnodeXcoord ,
         'anodeycoord':l_AnodeYcoord ,
         'bnodexcoord':l_BnodeXcoord ,
         'bnodeycoord':l_BnodeYcoord
        #  ,
        #  'id':l_id_1
        })
    

    # - MainTable
    print("Loading MainTable to Pandas dataframe...")
    
    MainTable = pd.DataFrame(
        {'operatorcode':l_OperatorCode,
         'servicenum':l_ServiceNum,
         'bm_routeid':l_BM_RouteID_2,
         'bm_startstopid':l_BM_StartStopID_2,
         'direction':l_Direction,
         'stopid':l_StopID,
         'depttime':l_DeptTime,
         'seq':l_Seq,
         'mon':l_Mon,
         'tue':l_Tue,
         'wed':l_Wed,
         'thur':l_Thu,
         'fri':l_Fri,
         'sat':l_Sat,
         'sun':l_Sun,
         'totalweekly':l_TotalWeekly
        })
    
    print(f"RouteLines1 size: {len(RouteLines1.index)} rows")
    print(f"MainTable size:   {len(MainTable.index)} rows")

    
# EMPTY temp list for RouteLines1 columns

    l_Region = []
    l_BMRouteID_1 = []
    l_BM_StartStopID_1 = []
    l_AnodeStopID = []
    l_BnodeStopID = []
    l_AnodeXcoord = []
    l_AnodeYcoord = []
    l_BnodeXcoord = []
    l_BnodeYcoord = []

# EMPTY temp list for MainTable columns

    l_OperatorCode = []
    l_ServiceNum = []
    l_BM_RouteID_2 = []
    l_BM_StartStopID_2 = []
    l_Direction = []
    l_StopID = []
    l_DeptTime = []
    l_Seq = []
    l_Mon = []
    l_Tue = []
    l_Wed = []
    l_Thu = []
    l_Fri = []
    l_Sat = []
    l_Sun = []
    l_TotalWeekly = []    
    # l_id_2 = []

    
#  Upload to db

    print("Uploading RouteLines1 df to PostgreSQL...")    
    # RouteLines1.to_sql('routelines1', conn, schema='rl' ,if_exists='append', method='multi', index = False) # SLOW!
    copy_from_stringio_progress(conn2, RouteLines1, db_schema+'.routelines1_tmp', ('id','region', 'bmrouteid', 'bm_startstopid', 'anodestopid', 'bnodestopid', 'anodexcoord', 'anodeycoord', 'bnodexcoord', 'bnodeycoord'))
        
    print("Uploading MainTable df to PostgreSQL...")    
    # MainTable.to_sql('maintable', conn, schema='rl' ,if_exists='append', method='multi', index = False) # SLOW!
    copy_from_stringio_progress(conn2, MainTable, db_schema+'.maintable_tmp', ('id','operatorcode','servicenum','bm_routeid','bm_startstopid','direction','stopid','depttime','seq','mon','tue','wed','thur','fri','sat','sun','totalweekly'))

def ptstops_update(conn, db_schema, pts_table):
    
    print("Updating 'PTStops_temp'...")
    
    conn.execute(f"INSERT INTO {db_schema}.{pts_table}_temp (naptanid, xcoord, ycoord) SELECT naptanid, xcoord, ycoord FROM {db_schema}.{pts_table} GROUP BY naptanid, xcoord, ycoord")
    conn.execute(f"CREATE INDEX {idx_no}_{pts_table}_tmp_idx ON {db_schema}.{pts_table}_temp USING btree (naptanid text_pattern_ops ASC NULLS LAST) TABLESPACE pg_default")

def rl1_update(conn, db_schema, pts_table):    

    print("Updating RouteLines1 RouteID ...")
    
    conn.execute(f"""CREATE TABLE {db_schema}.routelines1 AS
                        SELECT  id, 
                                region, 
                                bmrouteid || '_' || ROUND(CAST(tmp.xcoord AS DOUBLE PRECISION)/1000) || '_' || ROUND(CAST(tmp.ycoord AS DOUBLE PRECISION)/1000) AS bmrouteid, 
                                bm_startstopid, 
                                anodestopid, 
                                bnodestopid 
                        FROM {db_schema}.routelines1_tmp, {db_schema}.{pts_table}_temp tmp
                        WHERE bm_startstopid = tmp.naptanid;""")

    conn.execute(f"DROP TABLE IF EXISTS {db_schema}.routelines1_tmp")

# kreiranje INDEX-a

    conn.execute(f"CREATE INDEX {idx_no}_rl1_bm_startstopid_idx ON {db_schema}.routelines1 USING btree (bm_startstopid text_pattern_ops ASC NULLS LAST) TABLESPACE pg_default")
    conn.execute("COMMIT")
    conn.execute(f"CREATE INDEX {idx_no}_rl1_bmrouteid_idx ON {db_schema}.routelines1 USING btree (bmrouteid text_pattern_ops ASC NULLS LAST) TABLESPACE pg_default")
    conn.execute("COMMIT")            

def mt_update(conn, db_schema, pts_table):    

    print("Updating Maintable RouteID...")
    
    conn.execute(f"""CREATE TABLE {db_schema}.maintable AS 
                        SELECT  id, operatorcode, servicenum, 
                                bm_routeid || '_' || ROUND(CAST(tmp.xcoord AS DOUBLE PRECISION)/1000) || '_' || ROUND(CAST(tmp.ycoord AS DOUBLE PRECISION)/1000) AS bm_routeid, 
                                bm_startstopid, direction, stopid, depttime, seq, mon, tue, wed, thur, fri, sat, sun, totalweekly
                        FROM {db_schema}.maintable_tmp, {db_schema}.{pts_table}_temp tmp
                        WHERE bm_startstopid = tmp.naptanid;""")
    
    conn.execute(f"DROP TABLE IF EXISTS {db_schema}.maintable_tmp")

# kreiranje INDEX-a
    
    conn.execute(f"CREATE INDEX {idx_no}_mt_bm_startstopid_idx ON {db_schema}.maintable USING btree (bm_startstopid text_pattern_ops ASC NULLS LAST)  TABLESPACE pg_default")
    conn.execute("COMMIT")    
    conn.execute(f"CREATE INDEX {idx_no}_mt_bm_routeid_idx ON {db_schema}.maintable USING btree (bm_routeid text_pattern_ops ASC NULLS LAST) TABLESPACE pg_default")
    conn.execute("COMMIT")


# ===========================================================================================================
# 2. CreateFreq
# ===========================================================================================================

def TempGetFreq(conn, db_schema, ColName, depttime1, depttime2):
    conn.execute('''CREATE TABLE '''+db_schema+'''.tempgetfreq AS SELECT * FROM '''+db_schema+'''.maintable WHERE ''' + ColName + ''' = 1 AND depttime >= ''' + str(depttime1) + ''' AND depttime < ''' + str(depttime2) + ''' AND seq = 1''')
    conn.execute('''CREATE TABLE '''+db_schema+'''.sumfreq AS SELECT bm_routeid, SUM(seq) AS sumfreq FROM '''+db_schema+'''.tempgetfreq GROUP BY bm_routeid''')

def UpdFreqVal(conn, db_schema, UpdCol, round_value):
    conn.execute('''UPDATE '''+db_schema+'''.rlfreq SET ''' + UpdCol + ''' = updfreqval.sumfreq/''' + str(round_value) + '''
                    FROM (SELECT '''+db_schema+'''.rlfreq.bm_routeid, operatorcode, servicenum, direction, operatorname,
                                monearly, monam, monbp, monep, monop, monnight,
                                tueearly, tueam, tuebp, tueep, tueop, tuenight, 
                                wedearly, wedam, wedbp, wedep, wedop, wednight,
                                thurearly, thuram, thurbp, thurep, thurop, thurnight,
                                friearly, friam, fribp, friep, friop, frinight,
                                satearly, satam, satbp, satep, satop, satnight, 
                                sunearly, sunam, sunbp, sunep, sunop, sunnight, 
                                id, sumfreq 
                        FROM '''+db_schema+'''.rlfreq, '''+db_schema+'''.sumfreq 
                        WHERE '''+db_schema+'''.rlfreq.bm_routeid = sumfreq.bm_routeid) AS updfreqval 
                    WHERE '''+db_schema+'''.rlfreq.id = updfreqval.id''')
    conn.execute('''UPDATE '''+db_schema+'''.rlfreq SET ''' + UpdCol + ''' = 0 WHERE ''' + UpdCol + ''' is NULL''')
    conn.execute('''UPDATE '''+db_schema+'''.rlfreq SET ''' + UpdCol + ''' = ROUND(CAST(''' + UpdCol + ''' AS numeric), 2)''')
    conn.execute('''DROP TABLE IF EXISTS '''+db_schema+'''.tempgetfreq''')
    conn.execute('''DROP TABLE IF EXISTS '''+db_schema+'''.sumfreq''')

def CreateFreq(conn, db_schema):
    
    ColNames = {1:'mon', 2:'tue', 3:'wed', 4:'thur', 5:'fri', 6:'sat', 7:'sun'}
    
    DepTimes = {
                'am':[2.0, 700,900],
                'bp':[7.0, 900,1600],
                'ep':[2.0, 1600,1800],
                'op':[6.0, 1800,2400],
                'night':[3.0, 0,300],
                'early':[3.0, 400,700]
               }    
    conn.execute(f"DROP TABLE IF EXISTS {db_schema}.rlfreq")    
    conn.execute('''CREATE TABLE '''+db_schema+'''.rlfreq(
       bm_routeid    text,
       operatorcode  text,
       servicenum    text,
       direction     text,
       operatorname  text,
       monearly      real,
       monam         real,
       monbp         real,
       monep         real,
       monop         real,
       monnight      real,
       tueearly      real,
       tueam         real,
       tuebp         real,
       tueep         real,
       tueop         real,
       tuenight      real,
       wedearly      real,
       wedam         real,
       wedbp         real,
       wedep         real,
       wedop         real,
       wednight      real,
       thurearly     real,
       thuram        real,
       thurbp        real,
       thurep        real,
       thurop        real,
       thurnight     real,
       friearly      real,
       friam         real,
       fribp         real,
       friep         real,
       friop         real,
       frinight      real,
       satearly      real,
       satam         real,
       satbp         real,
       satep         real,
       satop         real,
       satnight      real,
       sunearly      real,
       sunam         real,
       sunbp         real,
       sunep         real,
       sunop         real,
       sunnight      real,
       id serial PRIMARY KEY) TABLESPACE pg_default''')
    
    conn.execute('''
                    INSERT INTO '''+db_schema+'''.rlfreq (bm_routeid, operatorcode, servicenum, direction)
                    SELECT bm_routeid, operatorcode, servicenum, direction
                    FROM '''+db_schema+'''.maintable 
                    GROUP BY bm_routeid, operatorcode, servicenum, direction 
                    ORDER BY bm_routeid
                    ''')

    conn.execute('''UPDATE '''+db_schema+'''.rlfreq 
                    SET operatorname = "OperatorPublicName"
                    FROM (SELECT * From '''+db_schema+'''.rlfreq, '''+db_schema+'''.noctable WHERE operatorcode = "NOCCODE" AND NOT operatorcode = '') AS updop
                    WHERE updop.id = '''+db_schema+'''.rlfreq.id''')
    
    for key1, value1 in ColNames.items():
        print("Freq. Calculations for " + value1 + "...")
        for key2, value2 in DepTimes.items():
    #       print(key1, "ColName=" + str(value1+key2), value2[0],value2[1],value2[2])
    #       print("TempGetFreq", str(value1), value2[1], value2[2])
            TempGetFreq(conn, db_schema, str(value1), value2[1], value2[2])
    #       print("UpdFreqVal", str(value1+key2), value2[0])
            UpdFreqVal(conn, db_schema, str(value1+key2), value2[0])    

# ===========================================================================================================
# 3. ABSorter
# ===========================================================================================================

def ABsorter(conn, db_schema, pts_table):
    
    print("Creation of ABNodes and updating...")
    
    conn.execute(f"DROP TABLE IF EXISTS {db_schema}.abnodes")
    conn.execute('''CREATE TABLE '''+db_schema+'''.abnodes(
                                            anodestopid text ,
                                            bnodestopid text ,
                                            anodexcoord integer,
                                            anodeycoord integer,
                                            bnodexcoord integer,
                                            bnodeycoord integer,
                                            id serial) TABLESPACE pg_default''')
                                        
    conn.execute('''ALTER TABLE '''+db_schema+'''.abnodes OWNER to postgres''')

    conn.execute('''INSERT INTO '''+db_schema+'''.abnodes (anodestopid, bnodestopid)
                    SELECT anodestopid, bnodestopid
                    FROM '''+db_schema+'''.routelines1 
                    GROUP BY anodestopid, bnodestopid''')

    conn.execute('''UPDATE '''+db_schema+'''.abnodes
                    SET anodexcoord = CAST(updanode.xcoord AS integer), anodeycoord = CAST(updanode.ycoord AS integer)
                    FROM (SELECT * FROM '''+db_schema+'''.abnodes, '''+db_schema+'''.'''+pts_table+'''_temp WHERE anodestopid = naptanid) AS updanode
                    WHERE updanode.id = abnodes.id''')

    conn.execute('''UPDATE '''+db_schema+'''.abnodes
                    SET bnodexcoord = CAST(updbnode.xcoord AS integer), bnodeycoord = CAST(updbnode.ycoord AS integer)
                    FROM (SELECT * FROM '''+db_schema+'''.abnodes, '''+db_schema+'''.'''+pts_table+'''_temp WHERE bnodestopid = naptanid) AS updbnode
                    WHERE updbnode.id = abnodes.id''')
    
def abnodesuk(conn, db_schema):

    print(f"{Fore.LIGHTCYAN_EX}[Creation of 'ABNodesUK']")

    conn.execute(f"DROP TABLE IF EXISTS {db_schema}.abnodesuk")
    conn.execute(f"CREATE TABLE {db_schema}.abnodesuk AS SELECT * FROM {db_schema}.abnodes")
    
    print(f"{Fore.GREEN}Preparation of 'abnodesuk' table for shortest path calculations...") 

    conn.execute(f"ALTER TABLE {db_schema}.abnodesuk ADD a_fraction double precision")
    conn.execute(f"ALTER TABLE {db_schema}.abnodesuk ADD b_fraction double precision")	
    conn.execute(f"ALTER TABLE {db_schema}.abnodesuk ADD a_edge integer")
    conn.execute(f"ALTER TABLE {db_schema}.abnodesuk ADD b_edge integer")	
    conn.execute(f"ALTER TABLE {db_schema}.abnodesuk ADD a_node integer")
    conn.execute(f"ALTER TABLE {db_schema}.abnodesuk ADD b_node integer")
    conn.execute(f"ALTER TABLE {db_schema}.abnodesuk ADD hops integer")
    conn.execute(f"ALTER TABLE {db_schema}.abnodesuk ADD buffer_m integer")
    conn.execute(f"ALTER TABLE {db_schema}.abnodesuk ADD a_edge_circ integer")
    conn.execute(f"ALTER TABLE {db_schema}.abnodesuk ADD b_edge_circ integer")	
    conn.execute(f"ALTER TABLE {db_schema}.abnodesuk ADD CONSTRAINT abuk_pkey PRIMARY KEY (id)")

    print(f"{Fore.GREEN}Index creation...")
    conn.execute(f"CREATE INDEX {idx_no}_astop_idx ON {db_schema}.abnodesuk (anodestopid)")
    conn.execute(f"CREATE INDEX {idx_no}_bstop_idx ON {db_schema}.abnodesuk (bnodestopid)")
    conn.execute(f"CREATE INDEX {idx_no}_abstop_idx ON {db_schema}.abnodesuk (anodestopid, bnodestopid)")
    conn.execute(f"CREATE INDEX {idx_no}_aedge_idx ON {db_schema}.abnodesuk (a_edge)")
    conn.execute(f"CREATE INDEX {idx_no}_bedge_idx ON {db_schema}.abnodesuk (b_edge)")
    

    print(f"{Fore.LIGHTGREEN_EX}Done!")



