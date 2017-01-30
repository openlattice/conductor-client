package com.kryptnostic.conductor.rpc.serializers;

import java.io.IOException;

import com.dataloom.edm.types.processors.RenameEntitySetProcessor;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

public class RenameEntitySetProcessorStreamSerializer
        implements SelfRegisteringStreamSerializer<RenameEntitySetProcessor> {

    @Override
    public void write( ObjectDataOutput out, RenameEntitySetProcessor object ) throws IOException {
        out.writeUTF( object.getName() );
    }

    @Override
    public RenameEntitySetProcessor read( ObjectDataInput in ) throws IOException {
        String newName = in.readUTF();
        return new RenameEntitySetProcessor( newName );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.RENAME_ENTITY_SET_PROCESSOR.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<RenameEntitySetProcessor> getClazz() {
        return RenameEntitySetProcessor.class;
    }
}