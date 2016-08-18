package com.kryptnostic.conductor.rpc;

import java.io.IOException;
import java.io.Serializable;

import org.junit.Test;

import com.kryptnostic.rhizome.hazelcast.serializers.BaseSerializerTest;

public class LambdaSerializersTest extends BaseSerializerTest<LambdaStreamSerializer, Runnable>
        implements Serializable {
    private static final long serialVersionUID = -8844481298074343953L;

    @Override
    protected LambdaStreamSerializer createSerializer() {
        return new LambdaStreamSerializer();
    }

    @Override
    protected Runnable createInput() {
        return (Runnable & Serializable) () -> System.out.println( "foo" );
    }

    @Override
    @Test(
        expected = AssertionError.class )
    public void testSerializeDeserialize() throws SecurityException, IOException {
        super.testSerializeDeserialize();
    }
}
