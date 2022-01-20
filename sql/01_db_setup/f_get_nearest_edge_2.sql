-- FUNCTION: pgr.get_nearest_edge_2(character varying, bigint)

-- DROP FUNCTION pgr.get_nearest_edge_2(character varying, bigint);

CREATE OR REPLACE FUNCTION pgr.get_nearest_edge_2(
	node character varying,
	graph_node bigint)
    RETURNS integer
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
BEGIN
RETURN (SELECT road.gid
		FROM pgr.openroads road
		WHERE road.source = $2
		   OR road.target = $2
		ORDER BY road.geom <-> (SELECT pgr.get_node_geom($1))
		LIMIT 1);
END
$BODY$;

ALTER FUNCTION pgr.get_nearest_edge_2(character varying, bigint)
    OWNER TO postgres;
