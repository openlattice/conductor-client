package com.openlattice.materializer

import com.openlattice.postgres.DataTables.LAST_WRITE
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.DATA


fun createForeignServer(
        foreignServer: String,
        foreignHost: String,
        foreignPort: Short,
        foreignDb: String
): String {
    return """
       CREATE SERVER IF NOT EXISTS $foreignServer 
        FOREIGN DATA WRAPPER postgres_fdw
        OPTIONS (host '$foreignHost', port '$foreignPort', dbname '$foreignDb')
    """.trimIndent()
}

fun createUserMapping(
        localUser: String,
        foreignServer: String,
        foreignUser: String,
        foreignPassword: String
): String {
    return """
    CREATE USER MAPPING IF NOT EXISTS FOR $localUser
        SERVER $foreignServer
        OPTIONS (user '$foreignUser', password '$foreignPassword')
""".trimIndent()
}

const val DELTAS_BATCH_SIZE = 64_000
/**
 * This collects 64K non-hard delete deltas from the data table and inserts them into foreign data wrapper.
 */
val deltasSql =
        """
           WITH deltas AS (
                    SELECT  ${ENTITY_SET_ID.name},
                            ${ID_VALUE.name},
                            ${ORIGIN_ID.name},
                            ${PROPERTY_TYPE_ID.name},
                            ${HASH.name},
                            ${LAST_WRITE.name},
                            ${LAST_PROPAGATE.name},
                            ${VERSION.name} from ${DATA.name} WHERE ${LAST_WRITE.name} < ${LAST_PROPAGATE.name} 
                    LIMIT $DELTAS_BATCH_SIZE ),
                insertions AS (
                    INSERT INTO $ SELECT * from deltas ),
                UPDATE deltas SET LAST_PROPAGATE = LAST_WRITE WHERE ID_VALUE = deltas.id, origin_id = deltas.origin_id, etc.... 
        """