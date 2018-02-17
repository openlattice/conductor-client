package com.dataloom.hazelcast.serializers;

import com.dataloom.linking.LinkingVertexKey;
import java.io.Serializable;

import com.dataloom.linking.LinkingEdge;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;
import java.util.UUID;

public class LinkingEdgeStreamSerializerTest
        extends AbstractStreamSerializerTest<LinkingEdgeStreamSerializer, LinkingEdge>
        implements Serializable {

    private static final long serialVersionUID = 1020885685339290580L;

    @Override
    protected LinkingEdge createInput() {
        UUID graphId = UUID.randomUUID();
        LinkingVertexKey src = new LinkingVertexKey( graphId, UUID.randomUUID());
        LinkingVertexKey dst = new LinkingVertexKey( graphId, UUID.randomUUID());
        return new LinkingEdge(src, dst);
    }

    @Override
    protected LinkingEdgeStreamSerializer createSerializer() {
        return new LinkingEdgeStreamSerializer();
    }

}