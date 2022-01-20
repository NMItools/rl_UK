-- ======================================================================================================
-- 2

-- ABNodesUK -> rl.abnodesuk -> rl.nodesuk

CREATE TABLE rl.nodesuk
(
    id_pk serial NOT NULL,
	stop_id character varying(20),
    x integer,
    y integer,
	node_id integer,
    edge_id integer,
	fraction float,
    geom geometry(Point,27700),	
    CONSTRAINT nodesuk_pk PRIMARY KEY (id_pk)
) 
TABLESPACE pg_default;

ALTER TABLE rl.nodesuk OWNER to postgres;

CREATE INDEX geom_idx ON rl.nodesuk USING gist (geom) TABLESPACE pg_default;
CREATE INDEX stop_idx ON rl.nodesuk USING btree (stop_id COLLATE pg_catalog."default" ASC NULLS LAST) TABLESPACE pg_default;

INSERT INTO rl.nodesuk (stop_id, x, y)
SELECT anodestopid, anodexcoord, anodeycoord FROM rl.abnodesuk;

INSERT INTO rl.nodesuk (stop_id, x, y)
SELECT bnodestopid, bnodexcoord, bnodeycoord FROM rl.abnodesuk;

DELETE FROM rl.nodesuk
WHERE id_pk IN
    (SELECT id_pk
     FROM 
        (SELECT id_pk, ROW_NUMBER() OVER( PARTITION BY stop_id ORDER BY id_pk ) AS row_num
         FROM rl.nodesuk ) t
	     WHERE t.row_num > 1 ); -- DELETE 545105 -- Query returned successfully in 5 secs 2 msec.		 

-- SVE OPERACIJE pod 2 (verdi): Query returned successfully in 1 min 16 secs.

-- ----------------------------------------------------------------------------------------------------------

-- Kreiranje POINT geometrije za čvorove na osnovu X i Y koordinata u tabeli: 
UPDATE rl.nodesuk SET geom = ST_MakePoint(x, y);  --UPDATE 326363 --Query returned successfully in 5 secs 650 msec.

-- Upis ID-a najbližeg čvora na grafu od bus stanice:
UPDATE rl.nodesuk SET node_id = rl.get_nearest_node(stop_id); 	--UPDATE 326363 --Query returned successfully in 55 secs 354 msec.(kući)
																

-- Upis ID-a najbliže geometrije/linije od bus stanice:
UPDATE rl.nodesuk SET edge_id = rl.get_nearest_edge(stop_id); 	--UPDATE 326363 --Query returned successfully in 55 secs 354 msec.(kući)
         														--UPDATE 326363 --Query returned successfully in  9 min 15 secs.  (verdi)

-- Upis sračunatog procenta na kom rastojanju se nalazi presečna tačka sa bus stanicom (u odnosu na na dužinu najbliže geometrije/linije):
UPDATE rl.nodesuk SET fraction = rl.fraction_calc(edge_id, stop_id); --UPDATE 326363 --Query returned successfully in 16 secs 232 msec.(kući)
																	 --UPDATE 326363 --Query returned successfully in 4 min 38 secs.(verdi)
																	 