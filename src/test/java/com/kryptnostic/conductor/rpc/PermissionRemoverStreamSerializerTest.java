package com.kryptnostic.conductor.rpc;

import com.dataloom.authorization.DelegatedPermissionEnumSet;
import com.dataloom.authorization.processors.PermissionRemover;
import com.dataloom.mapstores.TestDataFactory;
import com.kryptnostic.conductor.rpc.serializers.PermissionRemoverStreamSerializer;
import com.kryptnostic.rhizome.hazelcast.serializers.BaseSerializerTest;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class PermissionRemoverStreamSerializerTest
        extends BaseSerializerTest<PermissionRemoverStreamSerializer, PermissionRemover> {
    @Override protected PermissionRemoverStreamSerializer createSerializer() {
        return new PermissionRemoverStreamSerializer();
    }

    @Override protected PermissionRemover createInput() {
        return new PermissionRemover( DelegatedPermissionEnumSet.wrap( TestDataFactory.permissions() ) );
    }
}
