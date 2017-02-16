package com.dataloom.hazelcast.serializers;

import java.io.Serializable;

import com.dataloom.linking.LinkingEdge;
import com.dataloom.linking.mapstores.LinkingEdgesMapstore;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

public class LinkingEdgeStreamSerializerTest
        extends AbstractStreamSerializerTest<LinkingEdgeStreamSerializer, LinkingEdge>
        implements Serializable {

    private static final long serialVersionUID = 1020885685339290580L;

    @Override
    protected LinkingEdge createInput() {
        return LinkingEdgesMapstore.testKey();
    }

    @Override
    protected LinkingEdgeStreamSerializer createSerializer() {
        return new LinkingEdgeStreamSerializer();
    }

}