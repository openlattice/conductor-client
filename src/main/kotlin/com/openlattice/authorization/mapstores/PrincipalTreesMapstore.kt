/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 *
 */

package com.openlattice.authorization.mapstores

import com.codahale.metrics.annotation.Timed
import com.google.common.collect.ImmutableList
import com.hazelcast.config.MapConfig
import com.hazelcast.config.MapStoreConfig
import com.kryptnostic.rhizome.mapstores.TestableSelfRegisteringMapStore
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.AclKeySet
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn.ACL_KEY
import com.openlattice.postgres.PostgresColumn.PRINCIPAL_OF_ACL_KEY
import com.openlattice.postgres.PostgresDatatype
import com.openlattice.postgres.PostgresTable.PRINCIPAL_TREES
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.streams.PostgresIterable
import com.openlattice.postgres.streams.StatementHolder
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.sql.ResultSet
import java.util.*
import java.util.function.Function
import java.util.function.Supplier

/**
 *
 * Quick and dirty mapstore for principal trees.
 */
private val insertSql = "INSERT INTO ${PRINCIPAL_TREES.name} (${ACL_KEY.name},${PRINCIPAL_OF_ACL_KEY.name}) " +
        "VALUES (?, ?) " +
        "ON CONFLICT DO NOTHING"
private val selectSql = "SELECT * FROM ${PRINCIPAL_TREES.name} WHERE ${ACL_KEY.name} <@ ? OR ${ACL_KEY.name} <@ ?"
private val deleteSql = "DELETE FROM ${PRINCIPAL_TREES.name} WHERE ${ACL_KEY.name}  <@ ? OR ${ACL_KEY.name} <@ ?"
private val deleteNotIn = "DELETE FROM ${PRINCIPAL_TREES.name} " +
        "WHERE ${ACL_KEY.name} = ? AND NOT( ${PRINCIPAL_OF_ACL_KEY.name} <@ ?) AND NOT ( ${PRINCIPAL_OF_ACL_KEY.name} <@ ? )"
private val logger = LoggerFactory.getLogger(PrincipalTreesMapstore::class.java)!!

@Service //This is here to allow this class to be automatically open for @Timed to work correctly
class PrincipalTreesMapstore(val hds: HikariDataSource) : TestableSelfRegisteringMapStore<AclKey, AclKeySet> {
    @Timed
    override fun storeAll(map: Map<AclKey, AclKeySet>) {
        hds.connection.use {
            val connection = it
            val ps2 = connection.prepareStatement(deleteNotIn)
            val ps = connection.prepareStatement(insertSql)
            map.forEach {
                val arrKey = connection.createArrayOf(
                        PostgresDatatype.UUID.sql(), (it.key as List<UUID>).toTypedArray()
                )

                val vMap = it.value.groupBy { it.size }

                it.value.forEach {
                    val arr1 = PostgresArrays.createUuidArrayOfArrays (connection, (vMap[1] ?: ImmutableList.of()).map{ (it as List<UUID>).toTypedArray() }.stream() )
                    val arr2 = PostgresArrays.createUuidArrayOfArrays (connection, (vMap[2] ?: ImmutableList.of()).map{ (it as List<UUID>).toTypedArray() }.stream() )

                    ps2.setObject(1, arrKey)
                    ps2.setArray(2, arr1)
                    ps2.setArray(3, arr2)
                    ps2.addBatch()

                    ps.setObject(1, arrKey)
                    ps.setArray(2, arr1)
                    ps.addBatch()
                    ps.setObject(1, arrKey)
                    ps.setArray(2, arr2)
                    ps.addBatch()
                }
            }
            ps2.executeBatch()
            ps.executeBatch()

        }

    }

    override fun loadAllKeys(): MutableIterable<AclKey> {
        return PostgresIterable<AclKey>(Supplier {
            logger.info("Load all iterator requested for ${this.mapName}")
            val connection = hds.connection
            val stmt = connection.createStatement()
            val rs = stmt.executeQuery("SELECT distinct(acl_key) from ${PRINCIPAL_TREES.name}")

            StatementHolder(connection, stmt, rs)
        }, Function<ResultSet, AclKey> { ResultSetAdapters.aclKey(it) }
        )
//        val keyList = keys.toMutableList()
//        logger.info("Keys: {}", keyList)
//        return keyList
    }

    @Timed
    override fun store(key: AclKey, value: AclKeySet) {
        storeAll(mapOf(key to value))
    }

    @Timed
    override fun loadAll(keys: Collection<AclKey>): MutableMap<AclKey, AclKeySet> {
        val keyMap = keys.groupBy { it.size }

        val data = PostgresIterable<Pair<AclKey, AclKey>>(
                Supplier {
                    val connection = hds.connection
                    val ps = connection.prepareStatement(selectSql)

                    for (i in 1..2) {
                        val arr = PostgresArrays.createUuidArrayOfArrays(
                                connection,
                                (keyMap[i] ?: ImmutableList.of()).map { (it as List<UUID>).toTypedArray() }.stream()
                        )
                        ps.setArray(i, arr)
                    }
                    StatementHolder(connection, ps, ps.executeQuery())
                },

                Function<ResultSet, Pair<AclKey, AclKey>> {
                    ResultSetAdapters.aclKey(it) to ResultSetAdapters.principalOfAclKey(
                            it
                    )
                }
        )
        val map: MutableMap<AclKey, AclKeySet> = mutableMapOf()
        data.forEach { map.getOrPut(it.first) { AclKeySet() }.add(it.second) }
        return map
    }

    @Timed
    override fun deleteAll(keys: Collection<AclKey>) {
        val keyMap = keys.groupBy { it.size }
        hds.connection.use {
            val connection = it
            it.prepareStatement(deleteSql).use {
                for (i in 1..2) {
                    val arr = PostgresArrays.createUuidArrayOfArrays(
                            connection,
                            (keyMap[i] ?: ImmutableList.of()).map { (it as List<UUID>).toTypedArray() }.stream()
                    )
                    it.setArray(i, arr)
                }
                it.executeUpdate()
            }
        }
    }

    @Timed
    override fun load(key: AclKey): AclKeySet? {
        val loaded = loadAll(listOf(key))
        return loaded[key]
    }

    @Timed
    override fun delete(key: AclKey) {
        deleteAll(listOf(key))
    }

    override fun generateTestKey(): AclKey {
        return TestDataFactory.aclKey()
    }

    override fun generateTestValue(): AclKeySet {
        return AclKeySet(ImmutableList.of(generateTestKey(), generateTestKey(), generateTestKey()))
    }

    override fun getMapStoreConfig(): MapStoreConfig {
        return MapStoreConfig()
                .setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER)
                .setImplementation(this)
                .setEnabled(true)
                .setWriteDelaySeconds(0)
    }

    override fun getMapName(): String {
        return HazelcastMap.PRINCIPAL_TREES.name;
    }

    override fun getTable(): String {
        return PRINCIPAL_TREES.name
    }

    override fun getMapConfig(): MapConfig {
        return MapConfig(mapName)
                .setMapStoreConfig(mapStoreConfig)
    }

    companion object {
        const val INDEX = "index[any]"
    }
}