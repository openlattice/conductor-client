package com.kryptnostic.conductor.rpc.serializers;

import java.io.IOException;

import org.springframework.stereotype.Component;

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.requests.processors.UpdateRequestStatusProcessor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class UpdateRequestStatusProcessorStreamSerializer
        implements SelfRegisteringStreamSerializer<UpdateRequestStatusProcessor> {

    @Override
    public void write( ObjectDataOutput out, UpdateRequestStatusProcessor object ) throws IOException {
        RequestStatusStreamSerializer.serialize( out, object.getRequestStatus() );
    }

    @Override
    public UpdateRequestStatusProcessor read( ObjectDataInput in ) throws IOException {
        return new UpdateRequestStatusProcessor( RequestStatusStreamSerializer.deserialize( in ) );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.UPDATE_REQUEST_STATUS_PROCESSOR.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<UpdateRequestStatusProcessor> getClazz() {
        return UpdateRequestStatusProcessor.class;
    }

}
