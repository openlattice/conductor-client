package com.dataloom.linking;

import com.dataloom.blocking.GraphEntityPair;
import com.dataloom.blocking.LinkingEntity;
import com.dataloom.data.EntityKey;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.linking.predicates.LinkingPredicates;
import com.dataloom.matching.FeatureExtractionAggregator;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
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
    private static final int blockSize   = 50;
    private static final boolean explain = false;

    private IMap<GraphEntityPair, LinkingEntity> linkingEntities;
    private HazelcastLinkingGraphs               linkingGraphs;
    private HazelcastInstance                    hazelcastInstance;

    public HazelcastBlockingService( HazelcastInstance hazelcastInstance ) {
        this.linkingEntities = hazelcastInstance.getMap( HazelcastMap.LINKING_ENTITIES.name() );
        this.linkingGraphs = new HazelcastLinkingGraphs( hazelcastInstance );
        this.hazelcastInstance = hazelcastInstance;
    }

    @Async
    public void blockAndMatch(
            GraphEntityPair graphEntityPair,
            LinkingEntity linkingEntity,
            Map<UUID, UUID> entitySetIdsToSyncIds,
            Map<FullQualifiedName, UUID> propertyTypeIdIndexedByFqn ) {
        IAtomicLong counter = hazelcastInstance.getAtomicLong( graphEntityPair.getGraphId().toString() );
        EntityKey[] eks = elasticsearchApi
                .executeEntitySetDataSearchAcrossIndices( entitySetIdsToSyncIds,
                        linkingEntity.getEntity(),
                        blockSize,
                        explain ).toArray( new EntityKey[] {} );

        linkingEntities.aggregate( new FeatureExtractionAggregator( graphEntityPair,
                        linkingEntity,
                        propertyTypeIdIndexedByFqn ),
                LinkingPredicates.entitiesFromKeysAndGraphId( eks, graphEntityPair.getGraphId() ) );
        counter.decrementAndGet();
    }
}
