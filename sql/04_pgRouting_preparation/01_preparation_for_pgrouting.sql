--	======================================================================================================================
--  1
--	KREIRANJE POTREBNIH TABELA ZA RAD

--	Imported network/road shapefile 
--	with "GENERATE SIMPLE GEOMETRIES INSTEAD OF MULTI" settings in "Shapefile and DBF Loader Exporter"
--	----------------------------------------------------------------------------------------------------------------------
-- 	Update your geometry type to 2D Linestring:
	
	ALTER TABLE rl.openroadsv2_polyline ALTER COLUMN geom TYPE geometry(LINESTRING, 27700) USING ST_Force2D(geom);
	
	--Query returned successfully in 42 secs 612 msec.

--	======================================================================================================================
--	Creation of network/road topology:

	ALTER TABLE rl.openroadsv2_polyline ADD COLUMN "source" integer;
	ALTER TABLE rl.openroadsv2_polyline ADD COLUMN "target" integer;
	SELECT pgr_createTopology('rl.openroadsv2_polyline', 0.000001, 'geom', 'gid', 'source', 'target');

	--NOTICE:  Performing checks, please wait .....
	--NOTICE:  Creating Topology, Please wait...
	--NOTICE:  3697000 edges processed
	--NOTICE:  -------------> TOPOLOGY CREATED FOR  3697587 edges
	--NOTICE:  Rows with NULL geometry or NULL id: 0
	--NOTICE:  Vertices table for table rl.openroadsv2_polyline is: rl.openroadsv2_polyline_vertices_pgr
	--NOTICE:  ----------------------------------------------
	--Successfully run. Total query runtime: 49 min 44 secs.
	--1 rows affected.
