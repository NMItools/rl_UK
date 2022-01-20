-- FUNCTION: pgr.ab_routes_calc_0(text, text, integer)

-- DROP FUNCTION pgr.ab_routes_calc_0(text, text, integer);

CREATE OR REPLACE FUNCTION pgr.ab_routes_calc_0(
	_val1 text,
	_val2 text,
	_buffer integer)
    RETURNS text
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
BEGIN
	EXECUTE 'INSERT INTO pgr.ab_routes
			SELECT *
			FROM (SELECT anodestopid, bnodestopid, anodexcoord, anodeycoord, bnodexcoord, bnodeycoord,
				(ST_LineSubstring((SELECT geom FROM pgr.openroads WHERE gid = a_edge), LEAST(a_fraction, b_fraction),GREATEST(a_fraction, b_fraction))),
				id, a_fraction, b_fraction, a_edge, b_edge, a_node, b_node, hops,' || ($3) ||' AS buffer_m, a_edge_circ, b_edge_circ
				FROM pgr.abnodesuk
				WHERE anodexcoord != 0 AND bnodexcoord != 0
					AND a_edge = b_edge
					AND id BETWEEN ' || ($1+1) || ' AND ' || ($2-1) || '
					) t;';
	RETURN 'Done!';
END
$BODY$;

ALTER FUNCTION pgr.ab_routes_calc_0(text, text, integer)
    OWNER TO postgres;
