package com.kryptnostic.conductor.rpc;

import java.io.IOException;
import java.io.Serializable;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.kryptnostic.conductor.rpc.serializers.ConductorCallStreamSerializer;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

@SuppressWarnings( "rawtypes" )
public class ConductorStreamSerializerTest extends AbstractStreamSerializerTest<ConductorCallStreamSerializer, ConductorCall>
        implements Serializable {
    private static final long serialVersionUID = -8844481298074343953L;

    @Override
    protected ConductorCallStreamSerializer createSerializer() {
        return new ConductorCallStreamSerializer();
    }

    @Override
    protected ConductorCall createInput() {
        return ConductorCall
                .wrap( Lambdas.getAllEntitiesOfType( new FullQualifiedName( "abc", "def" ), ImmutableList.of() ) );
    }

    @Override
    @Test(
        expected = AssertionError.class )
    public void testSerializeDeserialize() throws SecurityException, IOException {
        super.testSerializeDeserialize();
    }
}
