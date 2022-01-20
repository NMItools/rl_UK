-- FUNCTION: pgr.get_shortest_path_3b(character varying, character varying, integer)

-- DROP FUNCTION pgr.get_shortest_path_3b(character varying, character varying, integer);

CREATE OR REPLACE FUNCTION pgr.get_shortest_path_3b(
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

b_edge_id int := (SELECT pgr.get_nearest_edge_2($2, b_node_id));

q1 text :='WITH route AS (
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
q3 text := E'' || 'end_edge AS (SELECT geom FROM pgr.openroads WHERE gid = (SELECT pgr.get_nearest_edge_2(''' || $2 || ''', ' || b_node_id || '))),';
q4 text := E'' || 'end_frac_1 AS (SELECT ST_LineLocatePoint((SELECT geom FROM end_edge),(SELECT pgr.get_node_geom(''' || $2 || ''')))),';
q5 text := E'' || 'end_frac_2 AS (SELECT CASE WHEN (SELECT source FROM route WHERE path_seq = (SELECT MAX(path_seq) FROM route)) = ' || b_node_id || '
					   -- 1
								   THEN ( SELECT CASE WHEN (SELECT edge FROM route WHERE path_seq = (SELECT MAX(path_seq) FROM route))=' || b_edge_id || ' 
													  -- a
													  THEN (SELECT ST_LineLocatePoint((SELECT geom FROM end_edge),
																					  (SELECT the_geom FROM pgr.openroads_vertices_pgr 
																					   WHERE id = (SELECT target FROM route WHERE path_seq = (SELECT MAX(path_seq) FROM route)))))
													  -- b
													  ELSE (SELECT ST_LineLocatePoint((SELECT geom FROM end_edge),
																					  (SELECT the_geom FROM pgr.openroads_vertices_pgr 
																					   WHERE id = (SELECT source FROM route WHERE path_seq = (SELECT MAX(path_seq) FROM route))))) 
													  END)
					   -- 2
								   ELSE ( SELECT CASE WHEN (SELECT edge FROM route WHERE path_seq = (SELECT MAX(path_seq) FROM route))=' || b_edge_id || '
													  -- a
													  THEN (SELECT ST_LineLocatePoint((SELECT geom FROM end_edge),
																					  (SELECT the_geom FROM pgr.openroads_vertices_pgr 
																					   WHERE id = (SELECT source FROM route WHERE path_seq = (SELECT MAX(path_seq) FROM route)))))
													  -- b			  
													  ELSE (SELECT ST_LineLocatePoint((SELECT geom FROM end_edge),
																					  (SELECT the_geom FROM pgr.openroads_vertices_pgr 
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
$BODY$;

ALTER FUNCTION pgr.get_shortest_path_3b(character varying, character varying, integer)
    OWNER TO postgres;
