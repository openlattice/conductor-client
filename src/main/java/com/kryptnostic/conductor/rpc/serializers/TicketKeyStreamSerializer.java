package com.kryptnostic.conductor.rpc.serializers;

import java.io.IOException;
import java.util.UUID;

import org.springframework.stereotype.Component;

import com.dataloom.data.TicketKey;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class TicketKeyStreamSerializer implements SelfRegisteringStreamSerializer<TicketKey> {

    @Override
    public void write( ObjectDataOutput out, TicketKey object ) throws IOException {
        out.writeUTF( object.getPrincipalId() );
        UUIDStreamSerializer.serialize( out, object.getTicket() );
    }

    @Override
    public TicketKey read( ObjectDataInput in ) throws IOException {
        String principalId = in.readUTF();
        UUID ticket = UUIDStreamSerializer.deserialize( in );
        return new TicketKey( principalId, ticket );
    }

    @Override
    public int getTypeId() {
       return StreamSerializerTypeIds.TICKET_KEY.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<TicketKey> getClazz() {
        return TicketKey.class;
    }

}
