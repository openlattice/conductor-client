package com.dataloom.hazelcast.serializers;

import java.io.IOException;
import java.util.UUID;

import org.springframework.stereotype.Component;

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.conductor.rpc.OrderedRPCKey;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class OrderedRPCKeyStreamSerializer implements SelfRegisteringStreamSerializer<OrderedRPCKey> {

    @Override
    public void write( ObjectDataOutput out, OrderedRPCKey object ) throws IOException {
        UUIDStreamSerializer.serialize( out, object.getRequestId() );
        out.writeDouble( object.getWeight() );        
    }

    @Override
    public OrderedRPCKey read( ObjectDataInput in ) throws IOException {
        UUID requestId = UUIDStreamSerializer.deserialize( in );
        double weight = in.readDouble();
        return new OrderedRPCKey( requestId, weight );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.ORDERED_RPC_KEY.ordinal();
    }

    @Override
    public void destroy() {
        
    }

    @Override
    public Class<? extends OrderedRPCKey> getClazz() {
        return OrderedRPCKey.class;
    }

}
