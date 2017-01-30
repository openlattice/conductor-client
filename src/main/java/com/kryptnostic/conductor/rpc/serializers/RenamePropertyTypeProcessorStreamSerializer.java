package com.kryptnostic.conductor.rpc.serializers;

import java.io.IOException;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.edm.types.processors.RenamePropertyTypeProcessor;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

public class RenamePropertyTypeProcessorStreamSerializer
        implements SelfRegisteringStreamSerializer<RenamePropertyTypeProcessor> {

    @Override
    public void write( ObjectDataOutput out, RenamePropertyTypeProcessor object ) throws IOException {
        FullQualifiedNameStreamSerializer.serialize( out, object.getFullQualifiedName() );
    }

    @Override
    public RenamePropertyTypeProcessor read( ObjectDataInput in ) throws IOException {
        FullQualifiedName newFqn = FullQualifiedNameStreamSerializer.deserialize( in );
        return new RenamePropertyTypeProcessor( newFqn );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.RENAME_PROPERTY_TYPE_PROCESSOR.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<RenamePropertyTypeProcessor> getClazz() {
        return RenamePropertyTypeProcessor.class;
    }

}
