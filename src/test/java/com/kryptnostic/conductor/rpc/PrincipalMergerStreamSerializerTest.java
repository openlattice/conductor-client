package com.kryptnostic.conductor.rpc;

import com.dataloom.mapstores.TestDataFactory;
import com.dataloom.organizations.processors.PrincipalMerger;
import com.google.common.collect.ImmutableSet;
import com.kryptnostic.conductor.rpc.serializers.PermissionMergerStreamSerializer;
import com.kryptnostic.conductor.rpc.serializers.PrincipalMergerStreamSerializer;
import com.kryptnostic.rhizome.hazelcast.serializers.BaseSerializerTest;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class PrincipalMergerStreamSerializerTest
        extends BaseSerializerTest<PrincipalMergerStreamSerializer, PrincipalMerger> {
    @Override protected PrincipalMergerStreamSerializer createSerializer() {
        return new PrincipalMergerStreamSerializer();
    }

    @Override protected PrincipalMerger createInput() {
        return new PrincipalMerger( ImmutableSet
                .of( TestDataFactory.userPrincipal(), TestDataFactory.userPrincipal() ) );
    }
}
