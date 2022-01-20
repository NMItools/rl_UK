-- FUNCTION: pgr.get_nearest_edge_1(character varying)

-- DROP FUNCTION pgr.get_nearest_edge_1(character varying);

CREATE OR REPLACE FUNCTION pgr.get_nearest_edge_1(
	node character varying)
    RETURNS integer
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
BEGIN
RETURN (SELECT road.gid
		FROM pgr.openroads road
		ORDER BY road.geom <-> (SELECT pgr.get_node_geom($1))
		LIMIT 1);
END
$BODY$;

ALTER FUNCTION pgr.get_nearest_edge_1(character varying)
    OWNER TO postgres;
