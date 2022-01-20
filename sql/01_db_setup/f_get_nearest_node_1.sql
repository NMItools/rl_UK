-- FUNCTION: pgr.get_nearest_node_1(character varying)

-- DROP FUNCTION pgr.get_nearest_node_1(character varying);

CREATE OR REPLACE FUNCTION pgr.get_nearest_node_1(
	node character varying)
    RETURNS integer
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE

edge_id int := (SELECT pgr.get_nearest_edge($1));

BEGIN
RETURN (SELECT n.id
		FROM pgr.openroads_vertices_pgr n
		WHERE n.id IN (SELECT source FROM pgr.openroads WHERE gid = edge_id)
		   OR n.id IN (SELECT target FROM pgr.openroads WHERE gid = edge_id)
		ORDER BY n.the_geom <-> (SELECT pgr.get_node_geom($1))
		LIMIT 1);
END
$BODY$;

ALTER FUNCTION pgr.get_nearest_node_1(character varying)
    OWNER TO postgres;
