-- FUNCTION: pgr.get_nearest_node_2(character varying)

-- DROP FUNCTION pgr.get_nearest_node_2(character varying);

CREATE OR REPLACE FUNCTION pgr.get_nearest_node_2(
	node character varying)
    RETURNS TABLE(id_n bigint) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
DECLARE

edge_id int := (SELECT pgr.get_nearest_edge($1));

BEGIN
RETURN QUERY 
	(SELECT n.id
		FROM pgr.openroads_vertices_pgr n
		WHERE n.id IN (SELECT source FROM pgr.openroads WHERE gid = edge_id)
		   OR n.id IN (SELECT target FROM pgr.openroads WHERE gid = edge_id)
		ORDER BY n.the_geom <-> (SELECT pgr.get_node_geom($1))
		LIMIT 2);
END
$BODY$;

ALTER FUNCTION pgr.get_nearest_node_2(character varying)
    OWNER TO postgres;
