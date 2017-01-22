package com.kryptnostic.conductor.rpc.serializers;

import java.io.IOException;

import org.springframework.stereotype.Component;

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.requests.RequestStatus;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class RequestStatusStreamSerializer implements SelfRegisteringStreamSerializer<RequestStatus> {

    private static RequestStatus[] values = RequestStatus.values();

    @Override
    public void write( ObjectDataOutput out, RequestStatus object ) throws IOException {

        serialize( out, object );
    }

    @Override
    public RequestStatus read( ObjectDataInput in ) throws IOException {
        return deserialize( in );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.REQUEST_STATUS.ordinal();
    }

    @Override
    public void destroy() {

    }

    @Override
    public Class<RequestStatus> getClazz() {
        return RequestStatus.class;
    }

    public static void serialize( ObjectDataOutput out, RequestStatus object ) throws IOException {
        out.writeInt( object.ordinal() );
    }

    public static RequestStatus deserialize( ObjectDataInput in ) throws IOException {
        return values[ in.readInt() ];
    }

}
