package com.dataloom.hazelcast.serializers;

import java.util.UUID;

import com.dataloom.edm.types.processors.AddDstEntityTypesToAssociationTypeProcessor;
import com.google.common.collect.Sets;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

public class AddDstEntityTypesToAssociationTypeProcessorStreamSerializerTest
        extends
        AbstractStreamSerializerTest<AddDstEntityTypesToAssociationTypeProcessorStreamSerializer, AddDstEntityTypesToAssociationTypeProcessor> {

    @Override
    protected AddDstEntityTypesToAssociationTypeProcessorStreamSerializer createSerializer() {
        return new AddDstEntityTypesToAssociationTypeProcessorStreamSerializer();
    }

    @Override
    protected AddDstEntityTypesToAssociationTypeProcessor createInput() {
        return new AddDstEntityTypesToAssociationTypeProcessor( Sets.newHashSet( UUID.randomUUID() ) );
    }

}
