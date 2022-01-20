-- FUNCTION: pgr.get_shortest_path_2(character varying, character varying, integer)

-- DROP FUNCTION pgr.get_shortest_path_2(character varying, character varying, integer);

CREATE OR REPLACE FUNCTION pgr.get_shortest_path_2(
	node_a character varying,
	node_b character varying,
	buffer integer)
    RETURNS TABLE(geom_ geometry) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
DECLARE     

a_node_id int := (SELECT id_pk FROM pgr.nodesuk WHERE stop_id = $1);
b_node_id int := (SELECT id_pk FROM pgr.nodesuk WHERE stop_id = $2);

a_node text := (SELECT ST_AsTEXT(geom) FROM pgr.nodesuk WHERE stop_id = $1);
b_node text := (SELECT ST_AsTEXT(geom) FROM pgr.nodesuk WHERE stop_id = $2);

q1 text :='WITH route AS (SELECT * FROM pgr_withPoints(''SELECT gid as id, source, target, st_length(geom) as cost FROM pgr.openroads 
 WHERE geom && ST_Expand(ST_Envelope(ST_Collect(ST_GeomFromText(''''' || a_node || '''''), ST_GeomFromText(''''' || b_node ||'''''))),'||$3||')''' || ',' ||E'
 ' || '''SELECT id_pk as pid, edge_id, fraction FROM pgr.nodesuk 
 WHERE geom && ST_Expand(ST_Envelope(ST_Collect(ST_GeomFromText(''''' || a_node || '''''), ST_GeomFromText(''''' || b_node ||'''''))),'||$3||')'', -' || a_node_id || ', -' || b_node_id || ', directed => false)),';
q2 text := E'' || 'start_edge AS (SELECT geom FROM pgr.openroads WHERE gid = (SELECT edge FROM route WHERE path_seq = 1)),';
q3 text := E'' || 'start_frac_1 AS (SELECT ST_LineLocatePoint((SELECT geom FROM start_edge),(SELECT pgr.get_node_geom('''|| $1 ||''')))),';
q4 text := E'' || 'start_frac_2 AS (SELECT ST_LineLocatePoint((SELECT geom FROM start_edge),(SELECT the_geom FROM pgr.openroads_vertices_pgr WHERE id = (SELECT node FROM route WHERE path_seq = (SELECT MIN(path_seq)+1 FROM route))))),';
q5 text := E'' || 'start_geom AS (SELECT ST_LineSubstring((SELECT geom FROM start_edge),(SELECT LEAST ((SELECT * FROM start_frac_1),(SELECT * FROM start_frac_2))),(SELECT GREATEST ((SELECT * FROM start_frac_1),(SELECT * FROM start_frac_2))))),';
q6 text := E'' || 'end_edge AS (SELECT geom FROM pgr.openroads WHERE gid = (SELECT edge FROM route WHERE path_seq = (SELECT MAX(path_seq)-1 FROM route))),';
q7 text := E'' || 'end_frac_1 AS (SELECT ST_LineLocatePoint((SELECT geom FROM end_edge),(SELECT pgr.get_node_geom('''|| $2 ||''')))),';
q8 text := E'' || 'end_frac_2 AS (SELECT ST_LineLocatePoint((SELECT geom FROM end_edge),(SELECT the_geom FROM pgr.openroads_vertices_pgr WHERE id = (SELECT node FROM route WHERE path_seq = (SELECT MAX(path_seq)-1 FROM route))))),';
q9 text := E'' || 'end_geom AS (SELECT ST_LineSubstring((SELECT geom FROM end_edge),(SELECT LEAST ((SELECT * FROM end_frac_1),(SELECT * FROM end_frac_2))),(SELECT GREATEST ((SELECT * FROM end_frac_1),(SELECT * FROM end_frac_2))))),';
q10 text := E'' || 'middle_edges AS (SELECT * FROM pgr.openroads, route WHERE pgr.openroads.gid=route.edge AND route.path_seq BETWEEN (SELECT MIN(route.path_seq)+1 FROM route) AND (SELECT MAX(route.path_seq)-2  FROM route)),';
q11 text := E'' || 'middle_geom AS (SELECT ST_LineMerge(ST_Union(geom)) AS geom FROM middle_edges)';
q12 text := E'' || 'SELECT ST_LineMerge(ST_Union(ARRAY[(SELECT * FROM start_geom),(SELECT * FROM middle_geom),(SELECT * FROM end_geom)]));';

query_ text:= q1 || q2 || q3 || q4 || q5 || q6 || q7 || q8  || q9 || q10 || q11 || q12;

BEGIN 
	RETURN QUERY 
	EXECUTE query_ USING $1, $2, $3;
END
$BODY$;

ALTER FUNCTION pgr.get_shortest_path_2(character varying, character varying, integer)
    OWNER TO postgres;
