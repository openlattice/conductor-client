package com.openlattice.hazelcast.serializers

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.data.UpdateType
import com.openlattice.data.storage.aws.S3EntityUpdateEntryProcessor
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.mapstores.TestDataFactory
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.springframework.stereotype.Component
import javax.inject.Inject

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
class S3EntityUpdateEntryProcessorStreamSerializer : TestableSelfRegisteringStreamSerializer<S3EntityUpdateEntryProcessor> {
    companion object {
        private val updateTypes = UpdateType.values()
    }

    @Inject
    private lateinit var mapper: ObjectMapper

    override fun generateTestValue(): S3EntityUpdateEntryProcessor {
        return S3EntityUpdateEntryProcessor(
                TestDataFactory.entities(1,
                                         setOf(
                                                 TestDataFactory.propertyType(EdmPrimitiveTypeKind.String),
                                                 TestDataFactory.propertyType(EdmPrimitiveTypeKind.String)
                                         ).associateBy { it.id }).values.first(), UpdateType.PartialReplace
        )
    }

    override fun getTypeId(): Int = StreamSerializerTypeIds.S3_ENTITY_UPDATE_ENTRY_PROCESSOR.ordinal


    override fun getClazz(): Class<out S3EntityUpdateEntryProcessor> = S3EntityUpdateEntryProcessor::class.java


    override fun write(out: ObjectDataOutput, obj: S3EntityUpdateEntryProcessor) {
        out.writeByteArray(mapper.writeValueAsBytes(obj.update))
        out.writeInt(obj.updateType.ordinal)
    }

    override fun read(input: ObjectDataInput): S3EntityUpdateEntryProcessor = S3EntityUpdateEntryProcessor(
            mapper.readValue(input.readByteArray()),
            updateTypes[input.readInt()]
    )

}