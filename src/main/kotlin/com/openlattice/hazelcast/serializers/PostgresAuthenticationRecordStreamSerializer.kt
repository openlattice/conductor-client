package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.postgres.PostgresAuthenticationRecord
import org.springframework.stereotype.Component

@Component
class PostgresAuthenticationRecordStreamSerializer : SelfRegisteringStreamSerializer<PostgresAuthenticationRecord> {

    companion object{
        fun serialize(output: ObjectDataOutput, obj: PostgresAuthenticationRecord) {
            output.writeUTF(obj.connectionType)
            output.writeUTF(obj.database)
            output.writeUTF(obj.username)
            output.writeUTF(obj.ipAddress)
            output.writeUTF(obj.ipMask)
            output.writeUTF(obj.authenticationMethod)
        }

        fun deserialize(input: ObjectDataInput): PostgresAuthenticationRecord {
            val connectionType = input.readUTF()
            val database = input.readUTF()
            val username = input.readUTF()
            val ipAddress = input.readUTF()
            val ipMask = input.readUTF()
            val authenticationMethod = input.readUTF()
            return PostgresAuthenticationRecord(connectionType, database, username,ipAddress, ipMask, authenticationMethod)
        }
    }

    override fun write(output: ObjectDataOutput, obj: PostgresAuthenticationRecord) {
        serialize(output, obj)
    }

    override fun read(input: ObjectDataInput): PostgresAuthenticationRecord {
        return deserialize(input)
    }

    override fun getClazz(): Class<out PostgresAuthenticationRecord> {
        return PostgresAuthenticationRecord::class.java
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.POSTGRES_AUTHENTICATION_RECORD.ordinal
    }
}