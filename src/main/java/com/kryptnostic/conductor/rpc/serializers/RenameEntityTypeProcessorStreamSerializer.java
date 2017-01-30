package com.kryptnostic.conductor.rpc.serializers;

import java.io.IOException;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.edm.types.processors.RenameEntityTypeProcessor;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

public class RenameEntityTypeProcessorStreamSerializer
        implements SelfRegisteringStreamSerializer<RenameEntityTypeProcessor> {

    @Override
    public void write( ObjectDataOutput out, RenameEntityTypeProcessor object ) throws IOException {
        FullQualifiedNameStreamSerializer.serialize( out, object.getFullQualifiedName() );
    }

    @Override
    public RenameEntityTypeProcessor read( ObjectDataInput in ) throws IOException {
        FullQualifiedName newFqn = FullQualifiedNameStreamSerializer.deserialize( in );
        return new RenameEntityTypeProcessor( newFqn );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.RENAME_ENTITY_TYPE_PROCESSOR.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<RenameEntityTypeProcessor> getClazz() {
        return RenameEntityTypeProcessor.class;
    }
}
