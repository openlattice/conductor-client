package com.dataloom.hazelcast.serializers;

import com.openlattice.apps.processors.RemoveAppTypesFromAppProcessor;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class RemoveAppTypesFromAppProcessorStreamSerializer implements
        SelfRegisteringStreamSerializer<RemoveAppTypesFromAppProcessor> {
    @Override public Class<? extends RemoveAppTypesFromAppProcessor> getClazz() {
        return RemoveAppTypesFromAppProcessor.class;
    }

    @Override public void write(
            ObjectDataOutput out, RemoveAppTypesFromAppProcessor object ) throws IOException {
        SetStreamSerializers.fastUUIDSetSerialize( out, object.getAppTypeIds() );
    }

    @Override public RemoveAppTypesFromAppProcessor read( ObjectDataInput in ) throws IOException {
        return new RemoveAppTypesFromAppProcessor( SetStreamSerializers.fastUUIDSetDeserialize( in ) );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.REMOVE_APP_TYPES_FROM_APP_PROCESSOR.ordinal();
    }

    @Override public void destroy() {

    }
}
