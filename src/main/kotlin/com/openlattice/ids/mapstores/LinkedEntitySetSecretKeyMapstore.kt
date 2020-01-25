/*
 * Copyright (C) 2019. OpenLattice, Inc.
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

package com.openlattice.ids.mapstores

import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.postgres.PostgresColumn
import com.openlattice.postgres.PostgresTable
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.mapstores.AbstractBasePostgresMapstore
import com.zaxxer.hikari.HikariDataSource
import org.apache.commons.lang.RandomStringUtils
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.*
import javax.crypto.spec.SecretKeySpec
import kotlin.random.Random


open class LinkedEntitySetSecretKeyMapstore(hds: HikariDataSource) : AbstractBasePostgresMapstore<UUID, SecretKeySpec>(
        HazelcastMap.LINKED_ENTITY_SET_SECRET_KEYS.name,
        PostgresTable.LINKED_ENTITY_SET_SECRET_KEYS,
        hds
) {
    override fun bind(ps: PreparedStatement, key: UUID, value: SecretKeySpec) {
        var index = bind(ps, key)
        ps.setString(index++, value.algorithm)
        ps.setBytes(index++, value.encoded)

        // shouldn't be updated once we store it
        ps.setString(index++, value.algorithm)
        ps.setBytes(index, value.encoded)
    }

    override fun bind(ps: PreparedStatement, key: UUID, offset: Int): Int {
        ps.setObject(offset, key)
        return offset + 1
    }

    override fun mapToKey(rs: ResultSet): UUID {
        return ResultSetAdapters.entitySetId(rs)
    }

    override fun mapToValue(rs: ResultSet): SecretKeySpec {
        val algorithm = rs.getString(PostgresColumn.ALGORITHM.name)
        val key = rs.getBytes(PostgresColumn.SECRET_KEY.name)

        return SecretKeySpec(key, algorithm)
    }

    override fun generateTestKey(): UUID {
        return UUID.randomUUID()
    }

    override fun generateTestValue(): SecretKeySpec {
        val algorithm = RandomStringUtils.random(10)
        val key = Random.nextBytes(32)

        return SecretKeySpec(key, algorithm)
    }
}