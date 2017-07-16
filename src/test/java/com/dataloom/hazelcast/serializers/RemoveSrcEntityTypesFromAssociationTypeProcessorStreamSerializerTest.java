package com.dataloom.hazelcast.serializers;

import java.util.UUID;

import com.dataloom.edm.types.processors.RemoveSrcEntityTypesFromAssociationTypeProcessor;
import com.google.common.collect.Sets;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

public class RemoveSrcEntityTypesFromAssociationTypeProcessorStreamSerializerTest
        extends
        AbstractStreamSerializerTest<RemoveSrcEntityTypesFromAssociationTypeProcessorStreamSerializer, RemoveSrcEntityTypesFromAssociationTypeProcessor> {

    @Override
    protected RemoveSrcEntityTypesFromAssociationTypeProcessorStreamSerializer createSerializer() {
        return new RemoveSrcEntityTypesFromAssociationTypeProcessorStreamSerializer();
    }

    @Override
    protected RemoveSrcEntityTypesFromAssociationTypeProcessor createInput() {
        return new RemoveSrcEntityTypesFromAssociationTypeProcessor( Sets.newHashSet( UUID.randomUUID() ) );
    }

}
