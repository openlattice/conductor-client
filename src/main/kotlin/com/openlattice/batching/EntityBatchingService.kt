package com.openlattice.batching

import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.BATCHES
import com.openlattice.postgres.PostgresTable.IDS
import com.openlattice.postgres.ResultSetAdapters
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import java.sql.PreparedStatement
import java.util.*

const val DEFAULT_BATCHING_SIZE = 8192

/**
 * Allow performing batch operations.
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class EntityBatchingService(val hds: HikariDataSource) {
    companion object {
        private val logger = LoggerFactory.getLogger(EntityBatchingService::class.java)
    }

    /**
     * Adds up to [batchSize] id to the batch of type [batchType] in the [BATCHES] table.
     *
     * @param batchType Use group types of batch operations together. An example here might be indexing vs linking indexing
     * @param idsSql A select sql fragment returns at least [ENTITY_SET_ID],[ID],[LINKING_ID], and [PARTITION] columns for batch entry that needs to be added.
     * @param batchSize The maximum number of batch entries to retry to read from executing idsSql and insert.
     * @param bind A bind function that takes a [PreparedStatement] and [Int] offset, performs the required binds for [idsSql] and returns the base after performing the binds.
     */
    fun enqueue(
            batchType: String,
            idsSql: String,
            batchSize: Int = DEFAULT_BATCHING_SIZE,
            bind: (PreparedStatement, Int) -> Int
    ) {
        hds.connection.use { connection ->
            connection.prepareStatement(buildInsertBatch(idsSql)).use { ps ->
                ps.setString(1, batchType)
                val offset = bind(ps, 1)
                ps.setInt(offset + 1, batchSize)
            }
        }
    }

    /**
     * Counts the number of batch entries of a given batch type.
     *
     * @param batchType Use group types of batch operations together. An example here might be indexing vs linking indexing
     */
    fun count(batchType: String): Long = hds.connection.use { connection ->
        connection.prepareStatement(COUNT_BATCH_TYPE).use { ps ->
            ps.setString(1, batchType)
            val rs = ps.executeQuery()
            require(rs.next()) { "Something went wrong when counting for batch type $batchType" }
            ResultSetAdapters.count(rs)
        }
    }

    /**
     * See [processBatch]
     */
    fun processBatch(
            batchType: String,
            batchSize: Int,
            process: (List<EntityBatchEntry>) -> Unit
    ) = processBatch<Unit>(batchType, batchSize, process)

    /**
     * @param batchType Use group types of batch operations together. An example here might be indexing vs linking indexing
     * @param batchSize The maximum number of batch entries to process.
     * @param process The function to applied to each batch.
     */
    fun <T> processBatch(
            batchType: String,
            batchSize: Int,
            process: (List<EntityBatchEntry>) -> T
    ): T = hds.connection.use { connection ->
        connection.autoCommit = false
        try {
            val batch = mutableListOf<EntityBatchEntry>()
            val result = connection.prepareStatement(SELECT_BATCH).use { ps ->
                ps.setString(1, batchType)
                ps.setInt(2, batchSize)
                val rs = ps.executeQuery()
                while (rs.next()) {
                    batch.add(
                            EntityBatchEntry(
                                    ResultSetAdapters.entitySetId(rs),
                                    ResultSetAdapters.id(rs),
                                    ResultSetAdapters.linkingId(rs),
                                    ResultSetAdapters.partition(rs),
                                    ResultSetAdapters.batchType(rs)
                            )
                    )
                }
                process(Collections.unmodifiableList(batch))
            }

            val deletedCount = connection.prepareStatement(DELETE_BATCH).use { ps ->
                batch.forEach { batchEntry ->
                    ps.setObject(1, batchEntry.id)
                    ps.setInt(2, batchEntry.partition)
                    ps.setString(3, batchType)
                    ps.addBatch()
                }
                ps.executeBatch()
            }.sum()

            connection.commit()
            logger.info("Successfully processed $deletedCount batch entries of type $batchType and size $batchSize.")

            result
        } catch (ex: Exception) {
            logger.error("Failed to process batch of type $batchType and size $batchSize.", ex)
            connection.autoCommit = true
            throw ex //Closes connection.
        }
    }


}

data class EntityBatchEntry(
        val entitySetId: UUID,
        val id: UUID,
        val linkingId: UUID?,
        val partition: Int,
        val batchType: String
)

private val COUNT_BATCH_TYPE = "SELECT count(*) FROM ${BATCHES.name} WHERE ${BATCH_TYPE.name} = ?"

private fun buildInsertBatch(idsSql: String) = """
    INSERT INTO ${BATCHES.name}
    SELECT ${IDS.name}.${ENTITY_SET_ID.name},${IDS.name}.${ID.name},${IDS.name}.${LINKING_ID.name},${IDS.name}.${PARTITION.name}, ? as bt
        FROM ($idsSql) as ids LEFT JOIN ${BATCHES.name} USING(${ID.name},${PARTITION.name})
        WHERE ${BATCHES.name}.${BATCH_TYPE.name} IS NULL
        LIMIT ?
    ON CONFLICT DO NOTHING
""".trimIndent()

private val DELETE_BATCH = """
    DELETE FROM ${BATCHES.name}
    WHERE ${ID_VALUE.name} = ? AND ${PARTITION.name} = ? AND ${BATCH_TYPE.name} = ?
""".trimIndent()
private val SELECT_BATCH = """
    SELECT * FROM ${BATCHES.name} 
    WHERE ${BATCH_TYPE.name} = ?
    ORDER BY ${ID_VALUE.name}
    LIMIT ?
    FOR UPDATE SKIP LOCKED
""".trimIndent()
