-- FUNCTION: pgr.ab_routes_table(text, text)

-- DROP FUNCTION pgr.ab_routes_table(text, text);

CREATE OR REPLACE FUNCTION pgr.ab_routes_table(
	_schema text,
	_table text)
    RETURNS text
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
                        _qual_tbl text := concat_ws('.', quote_ident(_schema), quote_ident(_table));
                        _row_found bool;
                        BEGIN
                        IF to_regclass(_qual_tbl) IS NOT NULL THEN
                            EXECUTE 'SELECT EXISTS (SELECT FROM ' || _qual_tbl || ')'
                            INTO _row_found;
                            IF _row_found THEN
                                EXECUTE 'TRUNCATE ' || _qual_tbl;
                                RETURN 'Table truncated: ' || _qual_tbl;
                            ELSE  
                                RETURN 'Table exists but is empty: ' || _qual_tbl;
                            END IF;
                        ELSE  
                            CREATE TABLE pgr.ab_routes
                            (
                                astopid character varying(20),
                                bstopid character varying(20),
                                axcoord integer,
                                aycoord integer,
                                bxcoord integer,
                                bycoord integer,
                                geom geometry(Geometry, 27700),
                                id integer,
                                a_fraction double precision,
                                b_fraction double precision,
                                a_edge integer,
                                b_edge integer,
                                a_node integer,
                                b_node integer,
                                hops integer,
                                buffer_m integer,
                                a_edge_circ integer,
                                b_edge_circ integer
                            );
                            RETURN 'Table created: ' || _qual_tbl;
                        END IF;
                        END
$BODY$;

ALTER FUNCTION pgr.ab_routes_table(text, text)
    OWNER TO postgres;
