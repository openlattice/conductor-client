package com.dataloom.hazelcast.serializers;

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.beust.jcommander.internal.Maps;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.mapstores.TestDataFactory;
import com.dataloom.matching.MatchingAggregator;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

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
        Map<UUID, PropertyType> authorizedPropertyTypes = Maps.newHashMap();
        Map<FullQualifiedName, String> propertyTypeFqns = Maps.newHashMap();
        Map<UUID, UUID> entitySetIdsToSyncIds = Maps.newHashMap();

        for ( int i = 0; i < 5; i++ ) {
            UUID id = UUID.randomUUID();
            PropertyType pt = TestDataFactory.propertyType();
            authorizedPropertyTypes.put( id, pt );
            propertyTypeFqns.put( pt.getType(), id.toString() );
            entitySetIdsToSyncIds.put( UUID.randomUUID(), UUID.randomUUID() );
        }
        return new MatchingAggregator(
                UUID.randomUUID(),
                entitySetIdsToSyncIds,
                authorizedPropertyTypes,
                propertyTypeFqns,
                new double[] { 0.4 },
                null );
    }

}
