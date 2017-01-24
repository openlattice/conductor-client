package com.kryptnostic.conductor.rpc.serializers;

import com.dataloom.authorization.Permission;
import com.dataloom.authorization.processors.PermissionMerger;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.EnumSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
@Component
public class PermissionMergerStreamSerializer implements SelfRegisteringStreamSerializer<PermissionMerger> {
    private static final Permission[] P = Permission.values();

    @Override public Class<? extends PermissionMerger> getClazz() {
        return PermissionMerger.class;
    }

    @Override public void write(
            ObjectDataOutput out, PermissionMerger object ) throws IOException {
        BitSet bs = new BitSet( P.length );
        for( Permission p : object.getBackingCollection() ) {
            bs.set( p.ordinal() );
        }
        out.writeLongArray( bs.toLongArray() );
    }

    @Override public PermissionMerger read( ObjectDataInput in ) throws IOException {
        BitSet bs = BitSet.valueOf( in.readLongArray() );

        EnumSet<Permission>  ps = EnumSet.noneOf(Permission.class);
        for( int i = 0 ; i < P.length ; ++i ) {
            if( bs.get( i ) ){
                ps.add( P[i ] );
            }
        }

        return new PermissionMerger(ps);
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.PERMISSION_MERGER.ordinal();
    }

    @Override public void destroy() {

    }
}
