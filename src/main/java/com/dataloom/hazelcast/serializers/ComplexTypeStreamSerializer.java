package com.dataloom.hazelcast.serializers;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.stereotype.Component;

import com.dataloom.edm.type.ComplexType;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.google.common.base.Optional;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class ComplexTypeStreamSerializer implements SelfRegisteringStreamSerializer<ComplexType> {

    @Override
    public void write( ObjectDataOutput out, ComplexType object ) throws IOException {
        UUIDStreamSerializer.serialize( out, object.getId() );
        FullQualifiedNameStreamSerializer.serialize( out, object.getType() );
        out.writeUTF( object.getTitle() );
        out.writeUTF( object.getDescription() );
        SetStreamSerializers.serialize( out,
                object.getSchemas(),
                FullQualifiedNameStreamSerializer::serialize );
        SetStreamSerializers.serialize( out,
                object.getProperties(),
                UUIDStreamSerializer::serialize );
        OptionalStreamSerializers.serialize( out, object.getBaseType(), UUIDStreamSerializer::serialize );
    }

    @Override
    public ComplexType read( ObjectDataInput in ) throws IOException {
        UUID id = UUIDStreamSerializer.deserialize( in );
        FullQualifiedName type = FullQualifiedNameStreamSerializer.deserialize( in );
        String title = in.readUTF();
        Optional<String> description = Optional.of( in.readUTF() );
        Set<FullQualifiedName> schemas = SetStreamSerializers.deserialize( in, FullQualifiedNameStreamSerializer::deserialize );
        LinkedHashSet<UUID> properties = SetStreamSerializers.orderedDeserialize( in,
                UUIDStreamSerializer::deserialize );
        Optional<UUID> baseType = OptionalStreamSerializers.deserialize( in, UUIDStreamSerializer::deserialize );

        return new ComplexType( id, type, title, description, schemas, properties, baseType );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.COMPLEX_TYPE.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<ComplexType> getClazz() {
        return ComplexType.class;
    }

}
