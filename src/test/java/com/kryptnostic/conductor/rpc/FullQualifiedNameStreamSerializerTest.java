package com.kryptnostic.conductor.rpc;

import java.io.Serializable;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.kryptnostic.conductor.rpc.serializers.FullQualifiedNameStreamSerializer;
import com.kryptnostic.rhizome.hazelcast.serializers.BaseSerializerTest;

public class FullQualifiedNameStreamSerializerTest
        extends BaseSerializerTest<FullQualifiedNameStreamSerializer, FullQualifiedName>
        implements Serializable {
    private static final long serialVersionUID = 6956722858352314361L;

    @Override
    protected FullQualifiedName createInput() {
        return new FullQualifiedName( "foo", "bar" );
    }

    @Override
    protected FullQualifiedNameStreamSerializer createSerializer() {
        return new FullQualifiedNameStreamSerializer();
    }

}
