package com.dataloom.hazelcast.serializers;

import java.util.UUID;

import com.kryptnostic.conductor.rpc.OrderedRPCKey;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

public class OrderedRPCKeyStreamSerializerTest extends AbstractStreamSerializerTest<OrderedRPCKeyStreamSerializer, OrderedRPCKey> {

    @Override
    protected OrderedRPCKeyStreamSerializer createSerializer() {
        return new OrderedRPCKeyStreamSerializer();
    }

    @Override
    protected OrderedRPCKey createInput() {
        return new OrderedRPCKey( UUID.randomUUID(), 1.0 );
    }

}
