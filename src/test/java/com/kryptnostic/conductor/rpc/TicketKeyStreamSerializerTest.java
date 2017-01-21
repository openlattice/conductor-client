package com.kryptnostic.conductor.rpc;

import java.util.UUID;

import org.apache.commons.lang3.RandomStringUtils;

import com.dataloom.data.TicketKey;
import com.kryptnostic.conductor.rpc.serializers.TicketKeyStreamSerializer;
import com.kryptnostic.rhizome.hazelcast.serializers.BaseSerializerTest;

public class TicketKeyStreamSerializerTest extends BaseSerializerTest<TicketKeyStreamSerializer, TicketKey> {

    @Override
    protected TicketKeyStreamSerializer createSerializer() {
        return new TicketKeyStreamSerializer();
    }

    @Override
    protected TicketKey createInput() {
        return new TicketKey( RandomStringUtils.random( 10 ), UUID.randomUUID() );
    }

}
