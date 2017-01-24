package com.kryptnostic.conductor.rpc;

import com.dataloom.mapstores.TestDataFactory;
import com.dataloom.organizations.processors.PrincipalMerger;
import com.dataloom.organizations.processors.PrincipalRemover;
import com.google.common.collect.ImmutableSet;
import com.kryptnostic.conductor.rpc.serializers.PermissionMergerStreamSerializer;
import com.kryptnostic.conductor.rpc.serializers.PrincipalMergerStreamSerializer;
import com.kryptnostic.conductor.rpc.serializers.PrincipalRemoverStreamSerializer;
import com.kryptnostic.rhizome.hazelcast.serializers.BaseSerializerTest;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class PrincipalRemoverStreamSerializerTest
        extends BaseSerializerTest<PrincipalRemoverStreamSerializer, PrincipalRemover> {
    @Override protected PrincipalRemoverStreamSerializer createSerializer() {
        return new PrincipalRemoverStreamSerializer();
    }

    @Override protected PrincipalRemover createInput() {
        return new PrincipalRemover( ImmutableSet
                .of( TestDataFactory.userPrincipal(), TestDataFactory.userPrincipal() ) );
    }
}
