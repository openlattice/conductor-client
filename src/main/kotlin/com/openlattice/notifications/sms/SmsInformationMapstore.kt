package com.openlattice.notifications.sms

import com.google.common.collect.ImmutableSet
import com.hazelcast.config.InMemoryFormat
import com.hazelcast.config.MapConfig
import com.hazelcast.config.MapIndexConfig
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresTable.SMS_INFORMATION
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.mapstores.AbstractBasePostgresMapstore
import com.zaxxer.hikari.HikariDataSource
import org.apache.commons.lang3.RandomStringUtils
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class SmsInformationMapstore(
        hds: HikariDataSource
) : AbstractBasePostgresMapstore<SmsInformationKey, SmsEntitySetInformation>(
        HazelcastMap.SMS_INFORMATION.name,
        SMS_INFORMATION,
        hds
) {
    companion object {
        const val ORGANIZATION_ID_INDEX = "__key#organizationId"
        const val PHONE_NUMBER_INDEX = "__key#phoneNumber"
        const val ANY_ENTITY_SET_ID_INDEX = "entitySetIds[any]"
        const val ANY_TAG_INDEX = "entitySetIds[any]"
    }

    override fun generateTestKey(): SmsInformationKey {
        return SmsInformationKey(RandomStringUtils.randomAlphanumeric(10), UUID.randomUUID())
    }

    override fun generateTestValue(): SmsEntitySetInformation {
        return SmsEntitySetInformation(
                RandomStringUtils.randomAlphanumeric(10),
                UUID.randomUUID(),
                ImmutableSet.of(UUID.randomUUID(), UUID.randomUUID()),
                ImmutableSet.of(RandomStringUtils.random(5), RandomStringUtils.random(5))
        )
    }

    override fun getMapConfig(): MapConfig {
        return super.getMapConfig()
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .addMapIndexConfig(MapIndexConfig(ORGANIZATION_ID_INDEX, false))
                .addMapIndexConfig(MapIndexConfig(PHONE_NUMBER_INDEX, false))
                .addMapIndexConfig(MapIndexConfig(ANY_ENTITY_SET_ID_INDEX, false))
                .addMapIndexConfig(MapIndexConfig(ANY_TAG_INDEX, false))
    }

    override fun bind(ps: PreparedStatement, key: SmsInformationKey, value: SmsEntitySetInformation) {
        var offset = bind(ps, key)

        ps.setArray(offset++, PostgresArrays.createUuidArray(ps.connection, value.entitySetIds))
        ps.setArray(offset, PostgresArrays.createTextArray(ps.connection, value.tags))
    }

    override fun mapToKey(rs: ResultSet): SmsInformationKey {
        return ResultSetAdapters.smsInformationKey(rs)
    }

    override fun bind(ps: PreparedStatement, key: SmsInformationKey, offset: Int): Int {
        ps.setObject(offset.inc(), key.phoneNumber)
        ps.setObject(offset, key.organizationId)
        return offset + 2
    }

    override fun mapToValue(rs: ResultSet): SmsEntitySetInformation {
        return ResultSetAdapters.smsEntitySetInformation(rs)
    }

}

