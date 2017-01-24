package com.kryptnostic.conductor.rpc;

import com.dataloom.authorization.DelegatedPermissionEnumSet;
import com.dataloom.authorization.processors.PermissionMerger;
import com.dataloom.mapstores.TestDataFactory;
import com.kryptnostic.conductor.rpc.serializers.PermissionMergerStreamSerializer;
import com.kryptnostic.rhizome.hazelcast.serializers.BaseSerializerTest;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class PermissionMergerStreamSerializerTest
        extends BaseSerializerTest<PermissionMergerStreamSerializer, PermissionMerger> {
    @Override protected PermissionMergerStreamSerializer createSerializer() {
        return new PermissionMergerStreamSerializer();
    }

    @Override protected PermissionMerger createInput() {
        return new PermissionMerger( DelegatedPermissionEnumSet.wrap( TestDataFactory.permissions() ) );
    }
}
