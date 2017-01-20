package com.kryptnostic.conductor.rpc.serializers;

import java.io.IOException;
import java.util.EnumSet;

import com.dataloom.authorization.DelegatedPermissionEnumSet;
import com.dataloom.authorization.Permission;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

public class DelegatedPermissionEnumSetStreamSerializer
        implements SelfRegisteringStreamSerializer<DelegatedPermissionEnumSet> {

    private static final Permission[] permissions = Permission.values();

    @Override
    public void write( ObjectDataOutput out, DelegatedPermissionEnumSet object ) throws IOException {
        serialize( out, object );
    }

    @Override
    public DelegatedPermissionEnumSet read( ObjectDataInput in ) throws IOException {
        return deserialize( in );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.PERMISSION_SET.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<DelegatedPermissionEnumSet> getClazz() {
        return DelegatedPermissionEnumSet.class;
    }

    public static void serialize( ObjectDataOutput out, DelegatedPermissionEnumSet object ) throws IOException {
        out.writeInt( object.size() );
        for ( Permission permission : object ) {
            out.writeInt( permission.ordinal() );
        }
    }

    public static DelegatedPermissionEnumSet deserialize( ObjectDataInput in ) throws IOException {
        int size = in.readInt();
        DelegatedPermissionEnumSet set = new DelegatedPermissionEnumSet();
        for ( int i = 0; i < size; ++i ) {
            set.add( permissions[ in.readInt() ] );
        }
        return set;
    }

}
