-- FUNCTION: pgr.get_edge_by_stopid(character varying)

-- DROP FUNCTION pgr.get_edge_by_stopid(character varying);

CREATE OR REPLACE FUNCTION pgr.get_edge_by_stopid(
	character varying)
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
						 WHERE stop_id = $1)
		   );
END
$BODY$;

ALTER FUNCTION pgr.get_edge_by_stopid(character varying)
    OWNER TO postgres;
