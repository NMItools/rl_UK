-- FUNCTION: pgr.fraction_calc(integer, character varying)

-- DROP FUNCTION pgr.fraction_calc(integer, character varying);

CREATE OR REPLACE FUNCTION pgr.fraction_calc(
	road integer,
	bus_stop character varying)
    RETURNS double precision
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
BEGIN
RETURN (WITH line_g AS (SELECT geom FROM pgr.openroads WHERE gid = $1),
			point_g AS (SELECT geom FROM pgr.nodesuk WHERE stop_id = $2)
			SELECT ST_LineLocatePoint((SELECT geom from line_g), (SELECT geom FROM point_g))
		);
END
$BODY$;

ALTER FUNCTION pgr.fraction_calc(integer, character varying)
    OWNER TO postgres;
