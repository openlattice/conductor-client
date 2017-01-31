package com.kryptnostic.conductor.rpc.serializers;

import java.io.IOException;
import java.util.Set;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.stereotype.Component;

import com.dataloom.edm.schemas.processors.AddSchemasToType;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class AddSchemasToTypeStreamSerializer implements SelfRegisteringStreamSerializer<AddSchemasToType> {
    @Override
    public void write( ObjectDataOutput out, AddSchemasToType object ) throws IOException {
        SetStreamSerializers.serialize( out, object.getSchemas(), ( FullQualifiedName schema ) -> {
            FullQualifiedNameStreamSerializer.serialize( out, schema );
        } );
    }

    @Override
    public AddSchemasToType read( ObjectDataInput in ) throws IOException {
        Set<FullQualifiedName> schemas = SetStreamSerializers.deserialize( in, ( ObjectDataInput dataInput ) -> {
            return FullQualifiedNameStreamSerializer.deserialize( dataInput );
        } );
        return new AddSchemasToType( schemas );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.ADD_SCHEMAS_TO_TYPE.ordinal();
    }

    @Override
    public void destroy() {
    }

    @Override
    public Class<AddSchemasToType> getClazz() {
        return AddSchemasToType.class;
    }
}
