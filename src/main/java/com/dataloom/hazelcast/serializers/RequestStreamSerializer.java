package com.dataloom.hazelcast.serializers;

import com.openlattice.authorization.AclKey;
import java.io.IOException;
import java.util.EnumSet;
import java.util.UUID;

import org.springframework.stereotype.Component;

import com.openlattice.authorization.Permission;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.openlattice.requests.Request;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.ListStreamSerializers;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class RequestStreamSerializer implements SelfRegisteringStreamSerializer<Request> {

    @Override
    public void write( ObjectDataOutput out, Request object ) throws IOException {
        serialize( out, object );
    }

    @Override
    public Request read( ObjectDataInput in ) throws IOException {
        return deserialize( in );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.REQUEST.ordinal();
    }

    @Override
    public void destroy() {

    }

    @Override
    public Class<Request> getClazz() {
        return Request.class;
    }

    public static void serialize( ObjectDataOutput out, Request object ) throws IOException {
        SetStreamSerializers.fastUUIDSetSerialize( out, object.getAclKey() );
        DelegatedPermissionEnumSetStreamSerializer.serialize( out, object.getPermissions() );
        out.writeUTF( object.getReason() );
    }

    public static Request deserialize( ObjectDataInput in ) throws IOException {
        UUID[] aclKey = ListStreamSerializers.fastUUIDArrayDeserialize( in );
        EnumSet<Permission> permissions = DelegatedPermissionEnumSetStreamSerializer.deserialize( in );
        String reason = in.readUTF();
        return new Request( new AclKey( aclKey ), permissions, reason );
    }
}
