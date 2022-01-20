-- FUNCTION: pgr.get_node_geom(character varying)

-- DROP FUNCTION pgr.get_node_geom(character varying);

CREATE OR REPLACE FUNCTION pgr.get_node_geom(
	node character varying)
    RETURNS geometry
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
BEGIN
	RETURN (SELECT geom	FROM pgr.nodesuk WHERE stop_id = $1);
END
$BODY$;

ALTER FUNCTION pgr.get_node_geom(character varying)
    OWNER TO postgres;
