package com.kryptnostic.conductor.rpc.serializers;

import com.dataloom.authorization.AclKey;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.kryptnostic.rhizome.hazelcast.objects.DelegatedUUIDList;
import com.kryptnostic.rhizome.hazelcast.serializers.ListStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import org.springframework.stereotype.Component;

import java.io.IOException;

import static com.kryptnostic.rhizome.hazelcast.serializers.ListStreamSerializers.DelegatedUUIDListStreamSerializer;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
@Component
public class AclKeyStreamSerializer extends DelegatedUUIDListStreamSerializer
        implements SelfRegisteringStreamSerializer<DelegatedUUIDList> {
    @Override
    public Class<? extends DelegatedUUIDList> getClazz() {
        return AclKey.class;
    }

    @Override public AclKey read( ObjectDataInput in ) throws IOException {
        return AclKey.wrap( ListStreamSerializers.fastUUIDListDeserialize( in ) );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.ACL_KEY.ordinal();
    }
}
