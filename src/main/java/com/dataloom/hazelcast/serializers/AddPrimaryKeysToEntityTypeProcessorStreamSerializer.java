package com.dataloom.hazelcast.serializers;

import com.dataloom.edm.types.processors.AddPrimaryKeysToEntityTypeProcessor;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class AddPrimaryKeysToEntityTypeProcessorStreamSerializer
        implements SelfRegisteringStreamSerializer<AddPrimaryKeysToEntityTypeProcessor> {
    @Override
    public Class<? extends AddPrimaryKeysToEntityTypeProcessor> getClazz() {
        return AddPrimaryKeysToEntityTypeProcessor.class;
    }

    @Override
    public void write(
            ObjectDataOutput out, AddPrimaryKeysToEntityTypeProcessor object ) throws IOException {
        SetStreamSerializers.fastUUIDSetSerialize( out, object.getPropertyTypeIds() );
    }

    @Override
    public AddPrimaryKeysToEntityTypeProcessor read( ObjectDataInput in ) throws IOException {
        return new AddPrimaryKeysToEntityTypeProcessor( SetStreamSerializers.fastUUIDSetDeserialize( in ) );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.ADD_PRIMARY_KEYS_TO_ENTITY_TYPE_PROCESSOR.ordinal();
    }

    @Override
    public void destroy() {
    }
}
