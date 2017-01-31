package com.kryptnostic.conductor.rpc.serializers;

import java.io.IOException;

import org.springframework.stereotype.Component;

import com.dataloom.edm.types.processors.AddPropertyTypesToEntityTypeProcessor;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class AddPropertyTypesToEntityTypeProcessorStreamSerializer implements SelfRegisteringStreamSerializer<AddPropertyTypesToEntityTypeProcessor> {

    @Override
    public void write( ObjectDataOutput out, AddPropertyTypesToEntityTypeProcessor object ) throws IOException {
        SetStreamSerializers.fastUUIDSetSerialize( out, object.getPropertyTypeIds() );
    }

    @Override
    public AddPropertyTypesToEntityTypeProcessor read( ObjectDataInput in ) throws IOException {
        return new AddPropertyTypesToEntityTypeProcessor( SetStreamSerializers.fastUUIDSetDeserialize( in ) );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.ADD_PROPERTY_TYPES_TO_ENTITY_TYPE_PROCESSOR.ordinal();
    }

    @Override
    public void destroy() {
    }

    @Override
    public Class<AddPropertyTypesToEntityTypeProcessor> getClazz() {
        return AddPropertyTypesToEntityTypeProcessor.class;
    }

}
