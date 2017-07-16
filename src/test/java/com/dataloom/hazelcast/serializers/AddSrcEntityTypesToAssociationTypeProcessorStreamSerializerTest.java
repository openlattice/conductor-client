package com.dataloom.hazelcast.serializers;

import java.util.UUID;

import com.dataloom.edm.types.processors.AddSrcEntityTypesToAssociationTypeProcessor;
import com.google.common.collect.Sets;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

public class AddSrcEntityTypesToAssociationTypeProcessorStreamSerializerTest
        extends
        AbstractStreamSerializerTest<AddSrcEntityTypesToAssociationTypeProcessorStreamSerializer, AddSrcEntityTypesToAssociationTypeProcessor> {

    @Override
    protected AddSrcEntityTypesToAssociationTypeProcessorStreamSerializer createSerializer() {
        return new AddSrcEntityTypesToAssociationTypeProcessorStreamSerializer();
    }

    @Override
    protected AddSrcEntityTypesToAssociationTypeProcessor createInput() {
        return new AddSrcEntityTypesToAssociationTypeProcessor( Sets.newHashSet( UUID.randomUUID() ) );
    }

}
