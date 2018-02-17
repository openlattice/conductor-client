package com.dataloom.hazelcast.serializers;

import com.openlattice.apps.processors.AddAppTypesToAppProcessor;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class AddAppTypesToAppProcessorStreamSerializer implements
        SelfRegisteringStreamSerializer<AddAppTypesToAppProcessor> {
    @Override public Class<? extends AddAppTypesToAppProcessor> getClazz() {
        return AddAppTypesToAppProcessor.class;
    }

    @Override public void write( ObjectDataOutput out, AddAppTypesToAppProcessor object ) throws IOException {
        SetStreamSerializers.fastUUIDSetSerialize( out, object.getAppTypeIds() );
    }

    @Override public AddAppTypesToAppProcessor read( ObjectDataInput in ) throws IOException {
        return new AddAppTypesToAppProcessor( SetStreamSerializers.fastUUIDSetDeserialize( in ) );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.ADD_APP_TYPES_TO_APP_PROCESSOR.ordinal();
    }

    @Override public void destroy() {

    }
}
