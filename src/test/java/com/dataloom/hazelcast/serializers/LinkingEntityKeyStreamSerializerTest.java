package com.dataloom.hazelcast.serializers;

import java.io.Serializable;
import java.util.UUID;

import com.dataloom.linking.LinkingEntityKey;
import com.openlattice.mapstores.TestDataFactory;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

public class LinkingEntityKeyStreamSerializerTest
        extends AbstractStreamSerializerTest<LinkingEntityKeyStreamSerializer, LinkingEntityKey>
        implements Serializable {

    private static final long serialVersionUID = -8980230293369103503L;

    @Override
    protected LinkingEntityKey createInput() {
        return new LinkingEntityKey( UUID.randomUUID(), TestDataFactory.entityKey() );
    }

    @Override
    protected LinkingEntityKeyStreamSerializer createSerializer() {
        return new LinkingEntityKeyStreamSerializer();
    }

}