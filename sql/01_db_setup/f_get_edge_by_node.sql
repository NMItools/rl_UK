-- FUNCTION: pgr.get_edge_by_node(integer)

-- DROP FUNCTION pgr.get_edge_by_node(integer);

CREATE OR REPLACE FUNCTION pgr.get_edge_by_node(
	integer)
    RETURNS geometry
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
BEGIN
	RETURN (SELECT r.geom AS geom 
			FROM pgr.openroads r 
			WHERE gid = (SELECT edge_id 
						 FROM pgr.nodesuk 
						 WHERE id_pk = $1));
END
$BODY$;

ALTER FUNCTION pgr.get_edge_by_node(integer)
    OWNER TO postgres;
