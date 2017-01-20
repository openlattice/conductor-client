package com.kryptnostic.conductor.rpc.serializers;

import java.io.IOException;

import org.springframework.stereotype.Component;

import com.dataloom.authorization.Principal;
import com.dataloom.authorization.PrincipalType;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class PrincipalStreamSerializer implements SelfRegisteringStreamSerializer<Principal> {

    private static final PrincipalType[] principalTypes = PrincipalType.values();

    @Override
    public void write( ObjectDataOutput out, Principal object ) throws IOException {
        serialize( out, object );
    }

    @Override
    public Principal read( ObjectDataInput in ) throws IOException {
        return deserialize( in );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.PRINCIPAL.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<Principal> getClazz() {
        return Principal.class;
    }

    public static void serialize( ObjectDataOutput out, Principal object ) throws IOException {
        out.writeInt( object.getType().ordinal() );
        out.writeUTF( object.getId() );
    }

    public static Principal deserialize( ObjectDataInput in ) throws IOException {
        PrincipalType type = principalTypes[ in.readInt() ];
        String id = in.readUTF();
        return new Principal( type, id );
    }

}
