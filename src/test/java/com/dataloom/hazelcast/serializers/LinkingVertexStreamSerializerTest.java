package com.dataloom.hazelcast.serializers;

import java.io.Serializable;

import com.dataloom.linking.LinkingVertex;
import com.dataloom.linking.mapstores.LinkingVerticesMapstore;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

public class LinkingVertexStreamSerializerTest
        extends AbstractStreamSerializerTest<LinkingVertexStreamSerializer, LinkingVertex>
        implements Serializable {

    private static final long serialVersionUID = -4065501215910619469L;

    @Override
    protected LinkingVertex createInput() {
        return LinkingVerticesMapstore.randomLinkingVertex();
    }

    @Override
    protected LinkingVertexStreamSerializer createSerializer() {
        return new LinkingVertexStreamSerializer();
    }

}