package com.kryptnostic.conductor.rpc.serializers;

import com.dataloom.authorization.DelegatedPermissionEnumSet;
import com.dataloom.authorization.Permission;
import com.dataloom.authorization.processors.PermissionMerger;
import com.dataloom.authorization.processors.PermissionRemover;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.BitSet;
import java.util.EnumSet;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
@Component
public class PermissionRemoverStreamSerializer implements SelfRegisteringStreamSerializer<PermissionRemover> {
    private static final Permission[] P = Permission.values();
    @Override public Class<? extends PermissionRemover> getClazz() {
        return PermissionRemover.class;
    }

    @Override public void write(
            ObjectDataOutput out, PermissionRemover object ) throws IOException {
        BitSet bs = new BitSet( P.length );
        for ( Permission p : object.getBackingCollection() ) {
            bs.set( p.ordinal() );
        }
        out.writeLongArray( bs.toLongArray() );

    }

    @Override public PermissionRemover read( ObjectDataInput in ) throws IOException {
        BitSet bs = BitSet.valueOf( in.readLongArray() );

        EnumSet<Permission> ps = EnumSet.noneOf( Permission.class );
        for ( int i = 0; i < P.length; ++i ) {
            if ( bs.get( i ) ) {
                ps.add( P[ i ] );
            }
        }

        return new PermissionRemover( ps );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.PERMISSION_REMOVER.ordinal();
    }

    @Override public void destroy() {

    }
}
