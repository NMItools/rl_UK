-- FUNCTION: pgr.get_shortest_path_1(character varying, character varying, integer, integer, integer)

-- DROP FUNCTION pgr.get_shortest_path_1(character varying, character varying, integer, integer, integer);

CREATE OR REPLACE FUNCTION pgr.get_shortest_path_1(
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
geom_a text := (SELECT ST_AsTEXT(geom) FROM pgr.nodesuk WHERE stop_id = $1);
geom_b text := (SELECT ST_AsTEXT(geom) FROM pgr.nodesuk WHERE stop_id = $2);

squery text :='SELECT ST_Union(geom) as geom_ FROM pgr.openroads 
	JOIN(SELECT seq, node, edge, cost FROM pgr_dijkstra(''SELECT gid as id, source, target, st_length(geom) as cost	FROM pgr.openroads 
	WHERE geom && ST_Expand(ST_Envelope(ST_Collect(ST_GeomFromText(''''' || geom_a || '''''), ST_GeomFromText(''''' || geom_b ||'''''))),'''''||$5||''''')''' || ', ' || $3 || ', ' || $4 || ',false)) AS route 
	ON pgr.openroads.gid = route.edge';

BEGIN 
	RETURN QUERY 
	EXECUTE squery USING a_node, b_node, geom_a, geom_b, a_nearest, b_nearest;
END
$BODY$;

ALTER FUNCTION pgr.get_shortest_path_1(character varying, character varying, integer, integer, integer)
    OWNER TO postgres;
