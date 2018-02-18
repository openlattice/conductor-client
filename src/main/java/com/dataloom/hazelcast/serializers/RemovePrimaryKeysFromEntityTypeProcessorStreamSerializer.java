package com.dataloom.hazelcast.serializers;

import com.dataloom.edm.types.processors.RemovePrimaryKeysFromEntityTypeProcessor;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

import java.io.IOException;

public class RemovePrimaryKeysFromEntityTypeProcessorStreamSerializer implements SelfRegisteringStreamSerializer<RemovePrimaryKeysFromEntityTypeProcessor> {
    @Override
    public Class<? extends RemovePrimaryKeysFromEntityTypeProcessor> getClazz() {
        return RemovePrimaryKeysFromEntityTypeProcessor.class;
    }

    @Override
    public void write(
            ObjectDataOutput out, RemovePrimaryKeysFromEntityTypeProcessor object ) throws IOException {
        SetStreamSerializers.fastUUIDSetSerialize( out, object.getPropertyTypeIds() );
    }

    @Override
    public RemovePrimaryKeysFromEntityTypeProcessor read( ObjectDataInput in ) throws IOException {
        return new RemovePrimaryKeysFromEntityTypeProcessor( SetStreamSerializers.fastUUIDSetDeserialize( in ) );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.REMOVE_PRIMARY_KEYS_FROM_ENTITY_TYPE_PROCESSOR.ordinal();
    }

    @Override
    public void destroy() {
    }
}
