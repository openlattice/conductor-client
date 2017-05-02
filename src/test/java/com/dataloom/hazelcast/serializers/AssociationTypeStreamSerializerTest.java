package com.dataloom.hazelcast.serializers;

import java.io.Serializable;

import com.dataloom.edm.type.AssociationType;
import com.dataloom.mapstores.TestDataFactory;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

public class AssociationTypeStreamSerializerTest
        extends AbstractStreamSerializerTest<AssociationTypeStreamSerializer, AssociationType>
        implements Serializable {
    private static final long serialVersionUID = 3237651698696026564L;

    @Override
    protected AssociationType createInput() {
        return TestDataFactory.associationType();
    }

    @Override
    protected AssociationTypeStreamSerializer createSerializer() {
        return new AssociationTypeStreamSerializer();
    }

}
