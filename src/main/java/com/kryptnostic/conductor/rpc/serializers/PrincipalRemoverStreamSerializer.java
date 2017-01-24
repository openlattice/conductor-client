package com.kryptnostic.conductor.rpc.serializers;

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.organizations.processors.PrincipalMerger;
import com.dataloom.organizations.processors.PrincipalRemover;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
@Component
public class PrincipalRemoverStreamSerializer implements SelfRegisteringStreamSerializer<PrincipalRemover> {
    @Override public Class<PrincipalRemover> getClazz() {
        return PrincipalRemover.class;
    }

    @Override public void write(
            ObjectDataOutput out, PrincipalRemover object ) throws IOException {
        SetStreamSerializers.serialize( out,
                object.getBackingCollection(),
                elem -> PrincipalStreamSerializer.serialize( out, elem ) );

    }

    @Override public PrincipalRemover read( ObjectDataInput in ) throws IOException {
        return new PrincipalRemover( SetStreamSerializers.deserialize( in , PrincipalStreamSerializer::deserialize ) );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.PRINCIPAL_REMOVER.ordinal();
    }

    @Override public void destroy() {

    }
}
