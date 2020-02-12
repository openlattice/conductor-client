package com.openlattice.materializer

/**
 *
 * The purpose of this class is to simply loop
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class MaterializerService {

    /**
     * Just loop until there
     *
     * As long as data lands before ids are added. We will add ids as necessary, if we cop
     */
    fun loop() {

    }
}


const val sql =
        """
            WITH dirty_ids AS (SELECT id, entity_set_id, organization_id FROM IDS LEFT JOIN materialized_views USING (entity_set_id) WHERE VERSION > 0 AND LAST_TRANSPORT < LAST_WRITE LIMIT 64000),
            insertions AS (INSERT INTO fdw SELECT * from dirty_ids)
            UPDATE IDS SET LAST_TRANSPORT = now() WHERE id in dirty_ids RETURNING id
        """