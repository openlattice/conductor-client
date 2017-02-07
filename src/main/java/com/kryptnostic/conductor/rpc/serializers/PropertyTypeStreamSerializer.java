package com.kryptnostic.conductor.rpc.serializers;

import java.io.IOException;
import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.stereotype.Component;

import com.dataloom.edm.internal.PropertyType;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.google.common.base.Optional;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class PropertyTypeStreamSerializer implements SelfRegisteringStreamSerializer<PropertyType> {

    private static final EdmPrimitiveTypeKind[] edmTypes = EdmPrimitiveTypeKind.values();

    @Override
    public void write( ObjectDataOutput out, PropertyType object ) throws IOException {
        UUIDStreamSerializer.serialize( out, object.getId() );
        FullQualifiedNameStreamSerializer.serialize( out, object.getType() );
        out.writeUTF( object.getTitle() );
        out.writeUTF( object.getDescription() );
        SetStreamSerializers.serialize( out, object.getSchemas(), ( FullQualifiedName schema ) -> {
            FullQualifiedNameStreamSerializer.serialize( out, schema );
        } );
        out.writeInt( object.getDatatype().ordinal() );
        out.writeBoolean( object.isPIIfield() );
    }

    @Override
    public PropertyType read( ObjectDataInput in ) throws IOException {
        UUID id = UUIDStreamSerializer.deserialize( in );
        FullQualifiedName type = FullQualifiedNameStreamSerializer.deserialize( in );
        String title = in.readUTF();
        Optional<String> description = Optional.of( in.readUTF() );
        Set<FullQualifiedName> schemas = SetStreamSerializers.deserialize( in, ( ObjectDataInput dataInput ) -> {
            return FullQualifiedNameStreamSerializer.deserialize( dataInput );
        } );
        EdmPrimitiveTypeKind datatype = edmTypes[ in.readInt() ];
        boolean piiField = in.readBoolean();
        return new PropertyType( id, type, title, description, schemas, datatype, piiField );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.PROPERTY_TYPE.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<PropertyType> getClazz() {
        return PropertyType.class;
    }

}
