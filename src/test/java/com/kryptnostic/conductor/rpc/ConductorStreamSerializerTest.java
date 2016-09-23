package com.kryptnostic.conductor.rpc;

import com.kryptnostic.conductor.rpc.serializers.ConductorCallStreamSerializer;
import com.kryptnostic.rhizome.hazelcast.serializers.BaseSerializerTest;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;

public class ConductorStreamSerializerTest extends BaseSerializerTest<ConductorCallStreamSerializer, ConductorCall>
        implements Serializable {
    private static final long serialVersionUID = -8844481298074343953L;

    @Override
    protected ConductorCallStreamSerializer createSerializer() {
        return new ConductorCallStreamSerializer(null);
    }

    @Override
    protected ConductorCall createInput() {
        return ConductorCall.wrap( Lambdas.getEntities( new FullQualifiedName( "abc","def" ) ) );
    }

    @Override
    @Test(
        expected = AssertionError.class )
    public void testSerializeDeserialize() throws SecurityException, IOException {
        super.testSerializeDeserialize();
    }
}
