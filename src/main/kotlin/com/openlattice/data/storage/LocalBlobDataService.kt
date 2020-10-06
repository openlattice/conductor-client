package com.openlattice.data.storage

import com.amazonaws.HttpMethod
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumnDefinition
import com.openlattice.postgres.PostgresDatatype
import com.openlattice.postgres.PostgresTableDefinition
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.PreparedStatementHolderSupplier
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.net.URL
import java.util.*

private val logger = LoggerFactory.getLogger(LocalBlobDataService::class.java)

private val TABLE = PostgresTableDefinition("mock_s3_bucket")
        .addColumns(
                PostgresColumnDefinition("key", PostgresDatatype.TEXT),
                PostgresColumnDefinition("object", PostgresDatatype.BYTEA)
        ).primaryKey(PostgresColumnDefinition("key", PostgresDatatype.TEXT))

@Service
class LocalBlobDataService(private val hds: HikariDataSource) : ByteBlobDataManager {
    init {
        hds.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(TABLE.createTableQuery())
            }
        }
    }

    override fun getPresignedUrl(
            key: Any, expiration: Date, httpMethod: HttpMethod, contentType: Optional<String>
    ): URL {
        throw UnsupportedOperationException()
    }

    override fun getPresignedUrls(keys: Collection<Any>): List<URL> {
        throw UnsupportedOperationException()
    }

    override fun getPresignedUrlsAsMap(keys: Collection<Any>): Map<Any, URL> {
        throw UnsupportedOperationException()
    }

    override fun putObject(s3Key: String, data: ByteArray, contentType: String) {
        insertEntity(s3Key, data)
    }

    override fun deleteObject(s3Key: String) {
        deleteEntity(s3Key)
    }

    override fun deleteObjects(s3Keys: List<String>) {
        s3Keys.forEach { deleteEntity(it) }
    }

    override fun getObjects(keys: Collection<Any>): List<Any> {
        return getObjectsAsMap(keys).values.toList()
    }

    override fun getObjectsAsMap(keys: Collection<Any>): Map<Any, Any> {
        return getEntitiesAsMap(keys)
    }

    private fun insertEntity(s3Key: String, value: ByteArray) {
        hds.connection.use { connection ->
            connection.prepareStatement(insertEntitySql).use { ps ->
                ps.setString(1, s3Key)
                ps.setBytes(2, value)
                ps.execute()
            }
        }
    }

    private fun deleteEntity(s3Key: String) {
        hds.connection.use { connection ->
            connection.prepareStatement(deleteEntitySql).use { ps ->
                ps.setString(1, s3Key)
                ps.executeUpdate()
            }
        }
    }


    private fun getEntitiesAsMap(keys: Collection<Any>): Map<Any, ByteArray> {
        return BasePostgresIterable(PreparedStatementHolderSupplier(hds, selectEntitySql) {
            it.setArray(1, PostgresArrays.createTextArray(it.connection, keys.map { k -> k.toString() }))
        }) {
            it.getString(1) to it.getBytes(2)
        }.toMap()
    }

    private val insertEntitySql = "INSERT INTO mock_s3_bucket(key, object) VALUES(?, ?)"

    private val deleteEntitySql = "DELETE FROM mock_s3_bucket WHERE key = ?"

    private val selectEntitySql = "SELECT key, \"object\" FROM mock_s3_bucket WHERE key = ANY(?)"

}