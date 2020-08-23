package com.openlattice.batching

import com.openlattice.data.storage.partitions.PartitionManager
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.BATCHES
import com.openlattice.postgres.PostgresTableDefinition
import com.openlattice.postgres.ResultSetAdapters
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.*

const val SELECT_INSERT_CLAUSE_NAME = "to_insert"
const val MARK_INSERTED_CLAUSE_NAME = "marked"

/**
 * When items can't be partitioned or require aggregation on the ids table. The main contractual observation here is
 * that we update last_write for involved objects in the same transaction that queues them using a CTE.
 *
 * @param batchTable The table to be used for concurrent work queues.
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
abstract class AggregateBatchingService<T>(
        protected val batchTable: PostgresTableDefinition,
        private val selectInsertBatchSql: String,
        private val markBatchSql: String,
        private val insertBatchSql: String,
        private val countSql: String,
        private val selectProcessBatchSql: String,
        private val selectDeleteBatchSql: String,
        private val hds: HikariDataSource,
        partitionManager: PartitionManager,
        private val mapper: (ResultSet) -> T
) {
    private val partitions = partitionManager.getAllPartitions()

    companion object {
        private val logger = LoggerFactory.getLogger(EntityBatchingService::class.java)
    }

    /**
     * Adds queue for us to see what the problem is
     *
     * @param batchType Use group types of batch operations together. An example here might be indexing vs linking indexing
     * @param idsSql A select sql fragment returns at least [ENTITY_SET_ID],[ID],[LINKING_ID], and [PARTITION] columns for batch entry that needs to be added.
     * @param batchSize The maximum number of batch entries to retry to read from executing idsSql and insert.
     * @param bind A bind function that takes a [PreparedStatement] and [Int] offset, performs the required binds for [idsSql] and returns the base after performing the binds.
     */
    fun enqueue(): Int = hds.connection.use { connection ->
        connection.prepareStatement(buildInsertBatch(selectInsertBatchSql, markBatchSql, insertBatchSql)).use { ps ->
            bindEnqueueBatch(ps)
            ps.executeUpdate()
        }
    }


    /**
     * Counts the number of batch entries.
     */
    fun count(): Long = hds.connection.use { connection ->
        connection.prepareStatement(countSql).use { ps ->
            bindQueueSize(ps)
            val rs = ps.executeQuery()
            require(rs.next()) { "Something went wrong when executing counting query." }
            ResultSetAdapters.count(rs)
        }
    }

    /**
     * @param batchSize The maximum number of batch entries to process.
     * @param process The function to applied to each batch.
     */
    fun <R> processBatch(
            batchSize: Int
    ): R = hds.connection.use { connection ->
        connection.autoCommit = false
        try {
            var remaining = batchSize
            val batch = mutableListOf<T>()

            /**
             * Since this is a skip locked query, it won't ever deadlock and will simply skipped locked rows to process
             * unlocked rows.
             */
            val result = connection.prepareStatement(selectProcessBatchSql).use { ps ->
                partitions.forEach { partition ->
                    bindProcessBatch(ps, partition, remaining)
                    val rs = ps.executeQuery()
                    while (rs.next()) {
                        batch.add(mapper(rs))
                    }
                    remaining = batchSize - batch.size //Don't read more entries than requested.
                    if (remaining == 0) {
                        return@forEach
                    }
                }
                process<R>(Collections.unmodifiableList(batch))
            }

            val deletedCount = connection.prepareStatement(selectDeleteBatchSql).use { ps ->
                batch.forEach { batchEntry ->
                    bindDequeueProcessBatch(ps, batchEntry)
                    ps.addBatch()
                }
                ps.executeBatch()
            }.sum()

            logger.info("Removed $deletedCount entries from queue.")

            connection.commit()
            logger.info("Committed processing of ${batch.size} entries of type with $batchSize.")
            result
        } catch (ex: Exception) {
            logger.error("Failed to process batch of size $batchSize.", ex)
            connection.rollback()

            throw ex //Closes connection.
        } finally {
            connection.autoCommit = true
        }
    }

    protected abstract fun <R> process( batch: List<T> ): R

    protected abstract fun bindEnqueueBatch(ps: PreparedStatement)
    protected abstract fun bindQueueSize(ps: PreparedStatement)
    protected abstract fun bindProcessBatch(ps: PreparedStatement, partition: Int, remaining: Int)
    protected abstract fun bindDequeueProcessBatch(ps: PreparedStatement, batch: T)
}

private fun buildInsertBatch(aggBatch: String, markSql: String, insertSql: String) = """
    WITH $SELECT_INSERT_CLAUSE_NAME as ($aggBatch),
        $MARK_INSERTED_CLAUSE_NAME as ($markSql)
    $insertSql
""".trimIndent()
