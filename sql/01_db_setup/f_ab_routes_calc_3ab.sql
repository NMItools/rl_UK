-- FUNCTION: pgr.ab_routes_calc_3ab(text, text, integer)

-- DROP FUNCTION pgr.ab_routes_calc_3ab(text, text, integer);

CREATE OR REPLACE FUNCTION pgr.ab_routes_calc_3ab(
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
				(SELECT geom_ FROM pgr.get_shortest_path_3ab(anodestopid, bnodestopid,' || ($3) ||')),
				id, a_fraction, b_fraction, a_edge, b_edge, a_node, b_node, hops,' || ($3) ||' AS buffer_m, a_edge_circ, b_edge_circ
				FROM pgr.abnodesuk
				WHERE anodexcoord != 0 AND bnodexcoord != 0 
				  AND a_edge != b_edge 
				  AND a_node = b_node
					AND ((a_fraction > 0 AND a_fraction < 1) AND (b_fraction = 0 OR b_fraction = 1) 
					OR (a_fraction = 0 OR a_fraction = 1)  AND (b_fraction > 0 AND b_fraction < 1))
				  AND id BETWEEN ' || ($1+1) || ' AND ' || ($2-1) || '
					) t;
			UPDATE pgr.ab_routes 
			SET geom = (SELECT geom_ FROM pgr.get_shortest_path_3ab(astopid, bstopid,' || 20*($3) ||'))
			   ,buffer_m = ' || 20*($3) ||'
				WHERE (geom IS NULL OR ST_GeometryType(geom) = ''ST_GeometryCollection'')
				  AND a_edge != b_edge 
				  AND a_node = b_node
					AND ((a_fraction > 0 AND a_fraction < 1) AND (b_fraction = 0 OR b_fraction = 1) 
					OR (a_fraction = 0 OR a_fraction = 1)  AND (b_fraction > 0 AND b_fraction < 1))
					AND id BETWEEN ' || ($1+1) || ' AND ' || ($2-1) || ';';
	RETURN 'Done!';
END
$BODY$;

ALTER FUNCTION pgr.ab_routes_calc_3ab(text, text, integer)
    OWNER TO postgres;
