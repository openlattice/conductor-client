package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.hazelcast.serializers.ListStreamSerializers
import com.kryptnostic.rhizome.hazelcast.serializers.RhizomeUtils
import com.openlattice.admin.ServiceDescription
import com.openlattice.admin.ServiceType
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
class ServiceDescriptionStreamSerializer : TestableSelfRegisteringStreamSerializer<ServiceDescription> {
    companion object {
        private val SERVICE_TYPES = ServiceType.values()
    }

    override fun generateTestValue(): ServiceDescription = ServiceDescription(ServiceType.REHEARSAL)

    override fun getTypeId(): Int = StreamSerializerTypeIds.SERVICE_DESCRIPTION.ordinal

    override fun getClazz(): Class<out ServiceDescription> {
        return ServiceDescription::class.java
    }

    override fun write(out: ObjectDataOutput, obj: ServiceDescription) {
        `out`.writeInt(obj.serviceType.ordinal)
        `out`.writeLong(obj.lastPing)

        ListStreamSerializers.serialize(`out`, obj.tags) { output, s -> output.writeUTF(s) }
    }

    override fun read(`in`: ObjectDataInput): ServiceDescription {
        val serviceType = SERVICE_TYPES[`in`.readInt()]
        val lastPing = `in`.readLong()
        val tags = ListStreamSerializers.deserialize(`in`) { input -> input.readUTF() }
        return ServiceDescription(serviceType,tags, lastPing)
    }
}