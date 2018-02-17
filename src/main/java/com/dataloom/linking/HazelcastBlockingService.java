package com.dataloom.linking;

import com.dataloom.blocking.GraphEntityPair;
import com.dataloom.blocking.LinkingEntity;
import com.openlattice.data.EntityKey;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.linking.predicates.LinkingPredicates;
import com.dataloom.matching.FeatureExtractionAggregator;
import com.google.common.collect.Sets;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.kryptnostic.conductor.rpc.ConductorElasticsearchApi;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.scheduling.annotation.Async;

import javax.inject.Inject;
import java.util.Map;
import java.util.UUID;

public class HazelcastBlockingService {

    @Inject
    private ConductorElasticsearchApi elasticsearchApi;

    private static final int     blockSize = 50;
    private static final boolean explain   = false;

    private IMap<GraphEntityPair, LinkingEntity> linkingEntities;
    private IMap<EntityKey, UUID>                ids;
    private HazelcastInstance                    hazelcastInstance;

    public HazelcastBlockingService( HazelcastInstance hazelcastInstance ) {
        this.linkingEntities = hazelcastInstance.getMap( HazelcastMap.LINKING_ENTITIES.name() );
        this.ids = hazelcastInstance.getMap( HazelcastMap.IDS.name() );
        this.hazelcastInstance = hazelcastInstance;
    }

    @Async
    public void blockAndMatch(
            GraphEntityPair graphEntityPair,
            LinkingEntity linkingEntity,
            Map<UUID, UUID> entitySetIdsToSyncIds,
            Map<FullQualifiedName, UUID> propertyTypeIdIndexedByFqn ) {
        UUID[] entityKeyIds = ids.getAll( Sets.newHashSet( elasticsearchApi
                .executeEntitySetDataSearchAcrossIndices( entitySetIdsToSyncIds,
                        linkingEntity.getEntity(),
                        blockSize,
                        explain ) ) ).values().toArray( new UUID[] {} );
        linkingEntities.aggregate( new FeatureExtractionAggregator( graphEntityPair,
                        linkingEntity,
                        propertyTypeIdIndexedByFqn ),
                LinkingPredicates.entitiesFromEntityKeyIdsAndGraphId( entityKeyIds, graphEntityPair.getGraphId() ) );
        hazelcastInstance.getCountDownLatch( graphEntityPair.getGraphId().toString() ).countDown();
    }
}
