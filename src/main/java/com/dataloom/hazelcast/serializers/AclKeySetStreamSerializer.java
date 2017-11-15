package com.dataloom.hazelcast.serializers;

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.AclKeySet;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Set;

@Component
public class AclKeySetStreamSerializer implements SelfRegisteringStreamSerializer<AclKeySet> {
    @Override public Class<? extends AclKeySet> getClazz() {
        return AclKeySet.class;
    }

    @Override public void write( ObjectDataOutput out, AclKeySet object ) throws IOException {
        SetStreamSerializers.serialize( out, object, aclKey -> {
            AclKeyStreamSerializer.serialize( out, aclKey );
        } );
    }

    @Override public AclKeySet read( ObjectDataInput in ) throws IOException {
        Set<AclKey> aclKeys = SetStreamSerializers.deserialize( in, AclKeyStreamSerializer::deserialize );
        return new AclKeySet( aclKeys );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.ACL_KEY_SET.ordinal();
    }

    @Override public void destroy() {

    }
}
