package com.kryptnostic.conductor.rpc.serializers;

import com.dataloom.authorization.DelegatedPermissionEnumSet;
import com.dataloom.authorization.processors.PermissionMerger;
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
public class PermissionMergerStreamSerializer implements SelfRegisteringStreamSerializer<PermissionMerger> {
    @Override public Class<? extends PermissionMerger> getClazz() {
        return PermissionMerger.class;
    }

    @Override public void write(
            ObjectDataOutput out, PermissionMerger object ) throws IOException {
        DelegatedPermissionEnumSet s = (DelegatedPermissionEnumSet) object.getBackingCollection();
        DelegatedPermissionEnumSetStreamSerializer.serialize( out, s.unwrap() );

    }

    @Override public PermissionMerger read( ObjectDataInput in ) throws IOException {
        return new PermissionMerger( DelegatedPermissionEnumSetStreamSerializer.deserialize( in ) );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.PERMISSION_MERGER.ordinal();
    }

    @Override public void destroy() {

    }
}
