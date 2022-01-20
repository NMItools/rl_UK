-- FUNCTION: pgr.get_shortest_path_4a(character varying, character varying, integer)

-- DROP FUNCTION pgr.get_shortest_path_4a(character varying, character varying, integer);

CREATE OR REPLACE FUNCTION pgr.get_shortest_path_4a(
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

geom_a text := (SELECT ST_AsTEXT(geom) FROM pgr.nodesuk WHERE stop_id = $1);
geom_b text := (SELECT ST_AsTEXT(geom) FROM pgr.nodesuk WHERE stop_id = $2);

a_node_id int := (SELECT node_id FROM pgr.nodesuk WHERE stop_id = $1);
b_node_id int := (SELECT node_id FROM pgr.nodesuk WHERE stop_id = $2);

a_edge_id int := (SELECT pgr.get_nearest_edge_2($1, a_node_id));q1 text :='WITH route AS (
				SELECT * 
				FROM pgr.openroads 
				JOIN(SELECT *
					FROM pgr_dijkstra(''SELECT gid as id, source, target, st_length(geom) as cost 
										FROM pgr.openroads 
										WHERE geom && ST_Expand(ST_Envelope(ST_Collect(ST_GeomFromText(''''' || geom_a || '''''), 
																						ST_GeomFromText(''''' || geom_b ||'''''))
																			),'||$3||')''' || 
										', ' || a_node_id || 
										', ' || b_node_id || 
										',false
									)
					) AS route ON pgr.openroads.gid = route.edge),';
q2 text := E'' || 'circ_edge AS (SELECT geom FROM pgr.openroads WHERE gid = ' || a_edge_id || '),';
q3 text := E'' || 'circ_frac_1 AS (SELECT ST_LineLocatePoint((SELECT geom FROM circ_edge),(SELECT pgr.get_node_geom(''' || $1 || ''')))),';
q4 text := E'' || 'circ_frac_2 AS (SELECT ST_LineLocatePoint((SELECT geom FROM circ_edge),(SELECT the_geom FROM pgr.openroads_vertices_pgr WHERE id = ' || a_node_id || '))),';
q5 text := E'' || 'circ_start AS (SELECT ST_LineSubstring((SELECT geom FROM circ_edge),(SELECT LEAST    ((SELECT * FROM circ_frac_1),(SELECT * FROM circ_frac_2))),(SELECT GREATEST ((SELECT * FROM circ_frac_1),(SELECT * FROM circ_frac_2)))))';
q6 text := E'' || 'SELECT ST_LineMerge(ST_Union(ARRAY[(SELECT * FROM circ_start),(SELECT ST_Union(geom) FROM route)]))';
query_ text:= q1 || q2 || q3 || q4 || q5 || q6;
BEGIN 
	-- RAISE NOTICE 'query_: %', query_;
	RETURN QUERY 
	EXECUTE query_ USING $1, $2, $3;
END
$BODY$;

ALTER FUNCTION pgr.get_shortest_path_4a(character varying, character varying, integer)
    OWNER TO postgres;
