package com.kryptnostic.conductor.rpc.serializers;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;

import org.springframework.stereotype.Component;

import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principal;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.requests.RequestStatus;
import com.dataloom.requests.Status;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class StatusStreamSerializer implements SelfRegisteringStreamSerializer<Status> {

    @Override
    public void write( ObjectDataOutput out, Status object ) throws IOException {
        PrincipalStreamSerializer.serialize( out, object.getPrincipal() );
        SetStreamSerializers.fastUUIDSetSerialize( out, object.getAclKey() );
        DelegatedPermissionEnumSetStreamSerializer.serialize( out, object.getPermissions() );
        RequestStatusStreamSerializer.serialize( out, object.getStatus() );

    }

    @Override
    public Status read( ObjectDataInput in ) throws IOException {
        Principal principal = PrincipalStreamSerializer.deserialize( in );
        List<UUID> aclKey = SetStreamSerializers.fastUUIDListDeserialize( in );
        EnumSet<Permission> permissions = DelegatedPermissionEnumSetStreamSerializer.deserialize( in );
        RequestStatus requestStatus = RequestStatusStreamSerializer.deserialize( in );
        return new Status( aclKey, principal, permissions, requestStatus );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.STATUS.ordinal();
    }

    @Override
    public void destroy() {

    }

    @Override
    public Class<Status> getClazz() {
        return Status.class;
    }

}
