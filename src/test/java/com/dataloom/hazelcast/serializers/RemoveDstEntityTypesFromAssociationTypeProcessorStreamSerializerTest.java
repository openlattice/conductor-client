package com.dataloom.hazelcast.serializers;

import java.util.UUID;

import com.dataloom.edm.types.processors.RemoveDstEntityTypesFromAssociationTypeProcessor;
import com.google.common.collect.Sets;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

public class RemoveDstEntityTypesFromAssociationTypeProcessorStreamSerializerTest
        extends
        AbstractStreamSerializerTest<RemoveDstEntityTypesFromAssociationTypeProcessorStreamSerializer, RemoveDstEntityTypesFromAssociationTypeProcessor> {

    @Override
    protected RemoveDstEntityTypesFromAssociationTypeProcessorStreamSerializer createSerializer() {
        return new RemoveDstEntityTypesFromAssociationTypeProcessorStreamSerializer();
    }

    @Override
    protected RemoveDstEntityTypesFromAssociationTypeProcessor createInput() {
        return new RemoveDstEntityTypesFromAssociationTypeProcessor( Sets.newHashSet( UUID.randomUUID() ) );
    }

}
