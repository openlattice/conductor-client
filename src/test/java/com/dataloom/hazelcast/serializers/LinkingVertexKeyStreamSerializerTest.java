package com.dataloom.hazelcast.serializers;

import java.io.Serializable;
import java.util.UUID;

import com.dataloom.linking.LinkingVertexKey;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

public class LinkingVertexKeyStreamSerializerTest
        extends AbstractStreamSerializerTest<LinkingVertexKeyStreamSerializer, LinkingVertexKey>
        implements Serializable {

    private static final long serialVersionUID = 6269984120456882013L;

    @Override
    protected LinkingVertexKey createInput() {
        return new LinkingVertexKey( UUID.randomUUID(), UUID.randomUUID() );
    }

    @Override
    protected LinkingVertexKeyStreamSerializer createSerializer() {
        return new LinkingVertexKeyStreamSerializer();
    }

}