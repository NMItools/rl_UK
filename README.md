Description
---
'RoutelinesUK'  (https://www.basemap.co.uk/map-data/route-lines/) is a command-line tool in Python for calculating the full BUS routes using shortest path calculation between BUS stop sequences in the UK and joining it with  matadata related to a BUS route (schedules, etc.). As input files, it uses an ATCO-CIF file and 'OS Open Roads' dataset from [Ordnance Survey Data Hub](https://osdatahub.os.uk/downloads/open/OpenRoads) which is imported into PostgreSQL database. The tool is using PostGIS/pgRouting extensions for PostgreSQL.

## ATCO-CIF

ATCO (Association of Transport Coordinating Officers) CIF (Common Interface File) was the United Kingdom standard for bus timetable data transfer for the first decade of the 2000s, but has since been largely replaced by TransXchange. CIF file contains UK timetabled public transport data and holds information from the Traveline National Dataset (TNDS) and Associate of Train Operating Companies (ATOC) as well as NAPTAN stop references.
* **Official spec:** [http://www.pti.org.uk/CIF/atco-cif-spec.pdf](http://www.pti.org.uk/CIF/atco-cif-spec.pdf)

Since it is a somewhat unfriendly and difficult to use format, this CLI tool reads **.cif** files and transform particular parts of data into SQLite/PostgreSQL table.
You can read here [short description of codes](https://github.com/NMItools/rluk/blob/main/docs/ATCO-CIF.sh)

![Screenshot 2021-11-24 13 54 26](https://user-images.githubusercontent.com/5813553/143242527-ff375ec1-8b75-4510-8e05-4e81b5daaeb0.png)

## OS Open Roads dataset
[OS Data Hub](https://osdatahub.os.uk) is the British Ordnance Survey platform for accessing UK geospatial data. The first release of the OS Data Hub was launched in July 2020 and replaces the OS OpenData Portal and the API shop for all users. 

[OS Open Roads](https://osdatahub.os.uk/downloads/open/OpenRoads) dataset is a high-level view of the road network, from motorways to country lane. Its available in ESRI® Shapefile, GML, GeoPackage, and Vector Tiles. This CLI tools imports ESRI® Shapefile into a PostgreSQL database and use PostGIS/pgRouting extensions for a shortest path calculations.

![Screenshot 2021-07-29 22 07 36_OS_data](https://user-images.githubusercontent.com/5813553/143242179-6b326b9d-5786-4957-a4af-20c43c782749.png)

## Screenshots

Loading SHP into a PostgreSQL:

![Screenshot 2021-07-23 10 30 58](https://user-images.githubusercontent.com/5813553/143242661-6c7ab662-4775-467b-8072-112d4e5df46b.png)

Calculating routes:

![Screenshot 2021-08-03 11 59 12](https://user-images.githubusercontent.com/5813553/143242641-b206c3e0-d3ad-4f07-9c89-e2e209188c0c.png)

A final calculated BUS route displayed over Google Maps:

![Screenshot 2021-05-10 14 45 48](https://user-images.githubusercontent.com/5813553/143242743-97bdb5f0-ea90-4b7b-bc44-700b7a20d6ce.png)


