package com.dataloom.hazelcast.serializers;

import com.dataloom.apps.processors.UpdateAppConfigEntitySetProcessor;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class UpdateAppConfigEntitySetProcessorStreamSerializer
        implements SelfRegisteringStreamSerializer<UpdateAppConfigEntitySetProcessor> {
    @Override public Class<? extends UpdateAppConfigEntitySetProcessor> getClazz() {
        return UpdateAppConfigEntitySetProcessor.class;
    }

    @Override public void write(
            ObjectDataOutput out, UpdateAppConfigEntitySetProcessor object ) throws IOException {
        UUIDStreamSerializer.serialize( out, object.getEntitySetId() );
    }

    @Override public UpdateAppConfigEntitySetProcessor read( ObjectDataInput in ) throws IOException {
        return new UpdateAppConfigEntitySetProcessor( UUIDStreamSerializer.deserialize( in ) );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.UPDATE_APP_CONFIG_ENTITY_SET_ID_PROCESSOR.ordinal();
    }

    @Override public void destroy() {

    }
}
