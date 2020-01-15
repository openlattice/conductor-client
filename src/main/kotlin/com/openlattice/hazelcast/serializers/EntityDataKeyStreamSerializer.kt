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
package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.data.EntityDataKey
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component
import java.io.IOException
import java.util.UUID

@Component
class EntityDataKeyStreamSerializer : TestableSelfRegisteringStreamSerializer<EntityDataKey> {

    companion object {
        @JvmStatic
        @Throws(IOException::class)
        fun serialize(output: ObjectDataOutput, obj: EntityDataKey) {
            UUIDStreamSerializer.serialize(output, obj.entityKeyId)
            UUIDStreamSerializer.serialize(output, obj.entitySetId)
        }

        @JvmStatic
        @Throws(IOException::class)
        fun deserialize(input: ObjectDataInput): EntityDataKey {
            val entityKeyId = UUIDStreamSerializer.deserialize(input)
            val entitySetId = UUIDStreamSerializer.deserialize(input)
            return EntityDataKey(entitySetId, entityKeyId)
        }
    }

    @Throws(IOException::class)
    override fun write(output: ObjectDataOutput, obj: EntityDataKey) {
        serialize(output, obj)
    }

    @Throws(IOException::class)
    override fun read(input: ObjectDataInput): EntityDataKey {
        return deserialize(input)
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.ENTITY_DATA_KEY.ordinal
    }

    override fun destroy() {}

    override fun getClazz(): Class<EntityDataKey> {
        return EntityDataKey::class.java
    }

    /* Test */
    override fun generateTestValue(): EntityDataKey {
        return EntityDataKey(UUID.randomUUID(), UUID.randomUUID())
    }
}