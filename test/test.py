
# interval = 42057

# for x in range(0, 420567, interval):
#     print(f"SELECT pgr.ab_routes_calc_0('{x}', '{x+interval}', 500);")


import psycopg2

conn = psycopg2.connect(dbname='test_pgr', user='postgres', password='softdesk', host='localhost', port='5434')
cursor = conn.cursor()

dir_cif = "D:/Routelines/data/"
sql = "COPY (SELECT stop_id FROM pgr.nodesuk WHERE geom is NULL) TO STDOUT WITH CSV DELIMITER ';'"
with open(f"{dir_cif}unknown_bus_stops.csv", "w") as file:
    cursor.copy_expert(sql, file)
