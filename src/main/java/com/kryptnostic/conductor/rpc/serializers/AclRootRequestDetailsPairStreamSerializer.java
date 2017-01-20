package com.kryptnostic.conductor.rpc.serializers;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import org.springframework.stereotype.Component;

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.requests.AclRootRequestDetailsPair;
import com.dataloom.requests.PermissionsRequestDetails;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class AclRootRequestDetailsPairStreamSerializer implements SelfRegisteringStreamSerializer<AclRootRequestDetailsPair> {

    @Override
    public void write( ObjectDataOutput out, AclRootRequestDetailsPair object ) throws IOException {
        StreamSerializerUtils.serializeFromList( out, object.getAclRoot(), ( UUID key ) -> {
            UUIDStreamSerializer.serialize( out, key );
        } );
        PermissionsRequestDetailsStreamSerializer.serialize( out, object.getDetails() );
    }

    @Override
    public AclRootRequestDetailsPair read( ObjectDataInput in ) throws IOException {
        List<UUID> aclRoot = StreamSerializerUtils.deserializeToList( in, ( ObjectDataInput dataInput ) -> {
            return UUIDStreamSerializer.deserialize( dataInput );
        } );
        PermissionsRequestDetails details = PermissionsRequestDetailsStreamSerializer.deserialize( in );
        return new AclRootRequestDetailsPair( aclRoot, details );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.ACLROOT_REQUEST_DETAILS_PAIR.ordinal();
    }

    @Override
    public void destroy() {
    }

    @Override
    public Class<AclRootRequestDetailsPair> getClazz() {
        return AclRootRequestDetailsPair.class;
    }

}
