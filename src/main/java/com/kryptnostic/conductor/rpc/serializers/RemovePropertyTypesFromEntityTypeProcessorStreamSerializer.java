package com.kryptnostic.conductor.rpc.serializers;

import java.io.IOException;

import org.springframework.stereotype.Component;

import com.dataloom.edm.types.processors.RemovePropertyTypesFromEntityTypeProcessor;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class RemovePropertyTypesFromEntityTypeProcessorStreamSerializer
        implements SelfRegisteringStreamSerializer<RemovePropertyTypesFromEntityTypeProcessor> {

    @Override
    public void write( ObjectDataOutput out, RemovePropertyTypesFromEntityTypeProcessor object ) throws IOException {
        SetStreamSerializers.fastUUIDSetSerialize( out, object.getPropertyTypeIds() );
    }

    @Override
    public RemovePropertyTypesFromEntityTypeProcessor read( ObjectDataInput in ) throws IOException {
        return new RemovePropertyTypesFromEntityTypeProcessor( SetStreamSerializers.fastUUIDSetDeserialize( in ) );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.REMOVE_PROPERTY_TYPES_FROM_ENTITY_TYPE_PROCESSOR.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<RemovePropertyTypesFromEntityTypeProcessor> getClazz() {
        return RemovePropertyTypesFromEntityTypeProcessor.class;
    }

}
