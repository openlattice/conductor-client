package com.kryptnostic.conductor.rpc.serializers;

import java.io.IOException;
import java.util.Set;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.stereotype.Component;

import com.dataloom.edm.schemas.processors.RemoveSchemasFromType;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class RemoveSchemasFromTypeStreamSerializer implements SelfRegisteringStreamSerializer<RemoveSchemasFromType> {

    @Override
    public void write( ObjectDataOutput out, RemoveSchemasFromType object ) throws IOException {
        SetStreamSerializers.serialize( out, object.getSchemas(), ( FullQualifiedName schema ) -> {
            FullQualifiedNameStreamSerializer.serialize( out, schema );
        } );
    }

    @Override
    public RemoveSchemasFromType read( ObjectDataInput in ) throws IOException {
        Set<FullQualifiedName> schemas = SetStreamSerializers.deserialize( in, ( ObjectDataInput dataInput ) -> {
            return FullQualifiedNameStreamSerializer.deserialize( dataInput );
        } );
        return new RemoveSchemasFromType( schemas );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.REMOVE_SCHEMAS_FROM_TYPE.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<RemoveSchemasFromType> getClazz() {
        return RemoveSchemasFromType.class;
    }
}
