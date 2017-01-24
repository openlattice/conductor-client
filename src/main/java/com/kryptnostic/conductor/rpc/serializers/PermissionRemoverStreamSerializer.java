package com.kryptnostic.conductor.rpc.serializers;

import com.dataloom.authorization.DelegatedPermissionEnumSet;
import com.dataloom.authorization.processors.PermissionMerger;
import com.dataloom.authorization.processors.PermissionRemover;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
@Component
public class PermissionRemoverStreamSerializer implements SelfRegisteringStreamSerializer<PermissionRemover> {
    @Override public Class<? extends PermissionRemover> getClazz() {
        return PermissionRemover.class;
    }

    @Override public void write(
            ObjectDataOutput out, PermissionRemover object ) throws IOException {
        DelegatedPermissionEnumSet s = (DelegatedPermissionEnumSet) object.getBackingCollection();
        DelegatedPermissionEnumSetStreamSerializer.serialize( out, s.unwrap() );

    }

    @Override public PermissionRemover read( ObjectDataInput in ) throws IOException {
        return new PermissionRemover( DelegatedPermissionEnumSetStreamSerializer.deserialize( in ) );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.PERMISSION_REMOVER.ordinal();
    }

    @Override public void destroy() {

    }
}
