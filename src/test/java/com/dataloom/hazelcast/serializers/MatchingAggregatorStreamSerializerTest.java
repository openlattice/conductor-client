package com.dataloom.hazelcast.serializers;

import com.dataloom.mapstores.TestDataFactory;
import com.dataloom.matching.MatchingAggregator;
import com.google.common.collect.Maps;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;

public class MatchingAggregatorStreamSerializerTest
        extends AbstractStreamSerializerTest<MatchingAggregatorStreamSerializer, MatchingAggregator>
        implements Serializable {
    private static final long serialVersionUID = -4292099784899244958L;

    @Override
    protected MatchingAggregatorStreamSerializer createSerializer() {
        return new MatchingAggregatorStreamSerializer();
    }

    @Override
    protected MatchingAggregator createInput() {
        Map<FullQualifiedName, UUID> propertyTypeFqns = Maps.newHashMap();

        for ( int i = 0; i < 5; i++ ) {
            propertyTypeFqns.put( TestDataFactory.propertyType().getType(), UUID.randomUUID() );
        }
        return new MatchingAggregator(
                UUID.randomUUID(),
                propertyTypeFqns,
                new double[] { 0.4 },
                null );
    }

}
