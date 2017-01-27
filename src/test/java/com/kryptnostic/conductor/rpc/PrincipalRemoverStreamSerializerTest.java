package com.kryptnostic.conductor.rpc;

import com.dataloom.mapstores.TestDataFactory;
import com.dataloom.organizations.processors.PrincipalRemover;
import com.google.common.collect.ImmutableSet;
import com.kryptnostic.conductor.rpc.serializers.PrincipalRemoverStreamSerializer;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class PrincipalRemoverStreamSerializerTest
        extends AbstractStreamSerializerTest<PrincipalRemoverStreamSerializer, PrincipalRemover> {
    @Override protected PrincipalRemoverStreamSerializer createSerializer() {
        return new PrincipalRemoverStreamSerializer();
    }

    @Override protected PrincipalRemover createInput() {
        return new PrincipalRemover( ImmutableSet
                .of( TestDataFactory.userPrincipal(), TestDataFactory.userPrincipal() ) );
    }
}
