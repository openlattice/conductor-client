package com.dataloom.matching;

import com.dataloom.blocking.BlockingAggregator;
import com.dataloom.blocking.GraphEntityPair;
import com.dataloom.blocking.LinkingEntity;
import com.dataloom.blocking.LoadingAggregator;
import com.dataloom.data.EntityKey;
import com.dataloom.data.hazelcast.DataKey;
import com.dataloom.data.hazelcast.EntitySets;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.linking.HazelcastLinkingGraphs;
import com.dataloom.linking.LinkingVertex;
import com.dataloom.linking.LinkingVertexKey;
import com.dataloom.linking.predicates.LinkingPredicates;
import com.google.common.base.Stopwatch;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.core.IMap;
import com.kryptnostic.datastore.services.EdmManager;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.scheduling.annotation.Async;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class DistributedMatcher {
    private EdmManager dms;

    private       SetMultimap<UUID, UUID>              linkIndexedByEntitySets;
    private       Map<UUID, UUID>                      linkingEntitySetsWithSyncId;
    private       Map<FullQualifiedName, UUID>         propertyTypeIdIndexedByFqn;
    private final IMap<DataKey, ByteBuffer>            data;
    private final IMap<GraphEntityPair, LinkingEntity> linkingEntities;
    private final HazelcastInstance                    hazelcast;
    private final HazelcastLinkingGraphs               graphs;

    public DistributedMatcher(
            HazelcastInstance hazelcast,
            EdmManager dms ) {
        this.data = hazelcast.getMap( HazelcastMap.DATA.name() );
        this.linkingEntities = hazelcast.getMap( HazelcastMap.LINKING_ENTITIES.name() );
        this.dms = dms;
        this.graphs = new HazelcastLinkingGraphs( hazelcast );
        this.hazelcast = hazelcast;
    }

    public double match( UUID graphId ) {
        Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypes = linkIndexedByEntitySets.asMap().entrySet().stream()
                .collect( Collectors.toMap( entry -> entry.getKey(),
                        entry -> entry.getValue().stream()
                                .collect( Collectors.toMap( ptId -> ptId, ptId -> dms.getPropertyType( ptId ) ) ) ) );

        Stopwatch s = Stopwatch.createStarted();

        graphs.initializeLinking( graphId, linkingEntitySetsWithSyncId );

        int numEntities = data.aggregate( new LoadingAggregator( graphId, authorizedPropertyTypes ),
                EntitySets.filterByEntitySetIdAndSyncIdPairs( linkingEntitySetsWithSyncId ) );
        System.out.println( "t1: " + String.valueOf( s.elapsed( TimeUnit.MILLISECONDS ) ) );
        s.reset();
        s.start();

        ICountDownLatch latch = hazelcast.getCountDownLatch( graphId.toString() );
        latch.trySetCount( numEntities );

        linkingEntities
                .aggregate( new BlockingAggregator( graphId, linkingEntitySetsWithSyncId, propertyTypeIdIndexedByFqn ),
                        LinkingPredicates.graphId( graphId ) );
        System.out.println( "t2: " + String.valueOf( s.elapsed( TimeUnit.MILLISECONDS ) ) );
        cleanLinkingEntitiesMap( graphId );
        latch.destroy();
        return 0.5;
    }

    @Async
    private void cleanLinkingEntitiesMap( UUID graphId ) {
        linkingEntities.removeAll( LinkingPredicates.graphId( graphId ) );
    }

    public void setLinking(
            Map<UUID, UUID> linkingEntitySetsWithSyncId,
            SetMultimap<UUID, UUID> linkIndexedByPropertyTypes,
            SetMultimap<UUID, UUID> linkIndexedByEntitySets ) {
        this.linkIndexedByEntitySets = linkIndexedByEntitySets;
        this.linkingEntitySetsWithSyncId = linkingEntitySetsWithSyncId;
        this.propertyTypeIdIndexedByFqn = getPropertyTypeIdIndexedByFqn( linkIndexedByPropertyTypes.keySet() );

    }

    private Map<FullQualifiedName, UUID> getPropertyTypeIdIndexedByFqn( Set<UUID> propertyTypeIds ) {
        return propertyTypeIds.stream()
                .collect( Collectors.toMap( id -> dms.getPropertyType( id ).getType(), id -> id ) );
    }

    static class Initializer extends Aggregator<Map.Entry<EntityKey, UUID>, Void>
            implements HazelcastInstanceAware {
        private static final long serialVersionUID = 3173601341356567535L;

        public UUID graphId;
        private transient IMap<LinkingVertexKey, LinkingVertex> linkingVertices;

        @Override public void accumulate( Map.Entry<EntityKey, UUID> input ) {
            linkingVertices.set( new LinkingVertexKey( graphId, input.getValue() ),
                    new LinkingVertex( 0.0D, Sets.newHashSet( input.getValue() ) ) );
        }

        @Override public void combine( Aggregator aggregator ) {

        }

        @Override public Void aggregate() {
            return null;
        }

        @Override public void setHazelcastInstance( HazelcastInstance hazelcastInstance ) {
            this.linkingVertices = hazelcastInstance.getMap( HazelcastMap.LINKING_VERTICES.name() );
        }
    }

}
