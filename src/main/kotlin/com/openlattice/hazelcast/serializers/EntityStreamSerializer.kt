package com.openlattice.hazelcast.serializers

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.IdConstants
import com.openlattice.IdConstants.LAST_WRITE_ID
import com.openlattice.IdConstants.VERSION_ID
import com.openlattice.data.Entity
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.mapstores.TestDataFactory
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.springframework.stereotype.Component
import java.time.OffsetDateTime
import javax.inject.Inject

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
class EntityStreamSerializer : TestableSelfRegisteringStreamSerializer<Entity> {
    @Inject
    private lateinit var mapper: ObjectMapper

    override fun generateTestValue(): Entity = Entity(
            TestDataFactory.entities(1,
                                     setOf(
                                             TestDataFactory.propertyType(
                                                     EdmPrimitiveTypeKind.String
                                             ),
                                             TestDataFactory.propertyType(EdmPrimitiveTypeKind.String)
                                     ).associateBy { it.id }).values.first()
    )

    override fun getTypeId(): Int = StreamSerializerTypeIds.ENTITY.ordinal

    override fun getClazz(): Class<out Entity> = Entity::class.java

    override fun write(out: ObjectDataOutput, obj: Entity) {
        out.writeByteArray(mapper.writeValueAsBytes(obj.properties))
    }

    override fun read(input: ObjectDataInput): Entity = Entity(mapper.readValue(input.readByteArray()))

}