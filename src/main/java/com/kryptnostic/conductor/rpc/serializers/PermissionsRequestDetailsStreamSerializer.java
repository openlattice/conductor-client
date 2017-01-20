package com.kryptnostic.conductor.rpc.serializers;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.UUID;

import org.springframework.stereotype.Component;

import com.dataloom.authorization.Permission;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.requests.PermissionsRequestDetails;
import com.dataloom.requests.RequestStatus;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class PermissionsRequestDetailsStreamSerializer
        implements SelfRegisteringStreamSerializer<PermissionsRequestDetails> {

    private static final RequestStatus[] status = RequestStatus.values();

    @Override
    public void write( ObjectDataOutput out, PermissionsRequestDetails object ) throws IOException {
        serialize( out, object );
    }

    @Override
    public PermissionsRequestDetails read( ObjectDataInput in ) throws IOException {
        return deserialize( in );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.PERMISSIONS_REQUEST_DETAILS.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<PermissionsRequestDetails> getClazz() {
        return PermissionsRequestDetails.class;
    }

    public static void serialize( ObjectDataOutput out, PermissionsRequestDetails object ) throws IOException {
        StreamSerializerUtils.serializeFromMap( out, object.getPermissions(), ( UUID key ) -> {
            UUIDStreamSerializer.serialize( out, key );
        }, ( EnumSet<Permission> permissions ) -> {
            StreamSerializerUtils.serializeFromPermissionEnumSet( out, permissions );
        } );
        out.writeInt( object.getStatus().ordinal() );
    }
    
    public static PermissionsRequestDetails deserialize( ObjectDataInput in ) throws IOException {
        Map<UUID, EnumSet<Permission>> permissions = StreamSerializerUtils.deserializeToMap( in,
                ( ObjectDataInput dataInput ) -> {
                    return UUIDStreamSerializer.deserialize( dataInput );
                },
                ( ObjectDataInput dataInput ) -> {
                    return StreamSerializerUtils.deserializeToPermissionEnumSet( dataInput );
                } );
        RequestStatus st = status[ in.readInt() ];
        return new PermissionsRequestDetails( permissions, st );
    }

}
