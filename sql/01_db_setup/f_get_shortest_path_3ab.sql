-- FUNCTION: pgr.get_shortest_path_3ab(character varying, character varying, integer)

-- DROP FUNCTION pgr.get_shortest_path_3ab(character varying, character varying, integer);

CREATE OR REPLACE FUNCTION pgr.get_shortest_path_3ab(
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

a_node_id int := (SELECT node_id FROM pgr.nodesuk WHERE stop_id = $1);
b_node_id int := (SELECT node_id FROM pgr.nodesuk WHERE stop_id = $2);

a_edge_id int := (SELECT pgr.get_nearest_edge($1));
b_edge_id int := (SELECT pgr.get_nearest_edge($2));

q1 text := E'' || 'WITH start_edge AS (SELECT geom FROM pgr.openroads WHERE gid = ' || a_edge_id || '),';
q2 text := E'' || 'start_frac_1 AS (SELECT ST_LineLocatePoint((SELECT geom FROM start_edge),(SELECT pgr.get_node_geom(''' || $1 || ''')))),';
q3 text := E'' || 'start_frac_2 AS (SELECT ST_LineLocatePoint((SELECT geom FROM start_edge),(SELECT the_geom FROM pgr.openroads_vertices_pgr WHERE id = ' || a_node_id || '))),';
q4 text := E'' || 'start_geom AS (SELECT ST_LineSubstring((SELECT geom FROM start_edge),(SELECT LEAST ((SELECT * FROM start_frac_1),(SELECT * FROM start_frac_2))),(SELECT GREATEST ((SELECT * FROM start_frac_1),(SELECT * FROM start_frac_2))))),';

q5 text := E'' || 'end_edge AS (SELECT geom FROM pgr.openroads WHERE gid = ' || b_edge_id || '),';
q6 text := E'' || 'end_frac_1 AS (SELECT ST_LineLocatePoint((SELECT geom FROM end_edge),(SELECT pgr.get_node_geom(''' || $2 || ''')))),';
q7 text := E'' || 'end_frac_2 AS (SELECT ST_LineLocatePoint((SELECT geom FROM end_edge),(SELECT the_geom FROM pgr.openroads_vertices_pgr WHERE id = ' || b_node_id || ' ))),';
q8 text := E'' || 'end_geom AS (SELECT ST_LineSubstring((SELECT geom FROM end_edge),(SELECT LEAST ((SELECT * FROM end_frac_1),(SELECT * FROM end_frac_2))),(SELECT GREATEST ((SELECT * FROM end_frac_1),(SELECT * FROM end_frac_2)))))';

q9 text := E'' || 'SELECT ST_LineMerge(ST_Union(ARRAY[(SELECT * FROM start_geom),(SELECT * FROM end_geom)]))';

query_ text:= q1 || q2 || q3 || q4 || q5 || q6 || q7 || q8 || q9;

BEGIN 
-- 	RAISE NOTICE 'query_: %', query_;
	RETURN QUERY 
	EXECUTE query_ USING $1, $2, $3;
END
$BODY$;

ALTER FUNCTION pgr.get_shortest_path_3ab(character varying, character varying, integer)
    OWNER TO postgres;
