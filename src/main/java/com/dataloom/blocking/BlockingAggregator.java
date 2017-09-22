package com.dataloom.blocking;

import com.dataloom.data.EntityKey;
import com.dataloom.data.hazelcast.DelegatedEntityKeySet;
import com.dataloom.hazelcast.HazelcastMap;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
import com.kryptnostic.conductor.rpc.ConductorElasticsearchApi;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class BlockingAggregator extends Aggregator<Map.Entry<GraphEntityPair, LinkingEntity>, Boolean>
        implements HazelcastInstanceAware {
    private           ConductorElasticsearchApi         elasticsearchApi;
    private           UUID                              graphId;
    private           Map<UUID, UUID>                   entitySetIdsToSyncIds;
    private transient IMap<DelegatedEntityKeySet, UUID> linkingEntityKeyIdPairs;
    private Map<DelegatedEntityKeySet, UUID> entityKeyIdPairs = Maps.newHashMap();
    Long   t0 = Long.valueOf( 0 );
    Long   t1 = Long.valueOf( 0 );
    double c0 = 0;
    double c1 = 0;

    // Number of search results taken in each block.
    private int     blockSize = 50;
    // Whether explanation for search results is stored.
    private boolean explain   = false;

    public BlockingAggregator( UUID graphId, Map<UUID, UUID> entitySetIdsToSyncIds ) {
        this( graphId, entitySetIdsToSyncIds, null );
    }

    public BlockingAggregator(
            UUID graphId,
            Map<UUID, UUID> entitySetIdsToSyncIds,
            ConductorElasticsearchApi elasticsearchApi ) {
        this.graphId = graphId;
        this.entitySetIdsToSyncIds = entitySetIdsToSyncIds;
        this.elasticsearchApi = elasticsearchApi;
    }

    @Override public void accumulate( Map.Entry<GraphEntityPair, LinkingEntity> input ) {
        EntityKey entityKey = input.getKey().getEntityKey();
        Stopwatch s = Stopwatch.createStarted();
        List<EntityKey> eks = elasticsearchApi
                .executeEntitySetDataSearchAcrossIndices( entitySetIdsToSyncIds,
                        input.getValue().getEntity(),
                        blockSize,
                        explain );
        t0 += s.elapsed( TimeUnit.MILLISECONDS );
        s.reset();
        s.start();
        eks.stream().forEach( otherEntityKey -> entityKeyIdPairs.put( DelegatedEntityKeySet.wrap( ImmutableSet.of( entityKey, otherEntityKey ) ), graphId) );

        t1 += s.elapsed( TimeUnit.MILLISECONDS );
        c0++;
        c1++;
    }

    @Override public void combine( Aggregator aggregator ) {
        if ( aggregator instanceof BlockingAggregator ) {
            BlockingAggregator other = (BlockingAggregator) aggregator;
            entityKeyIdPairs.putAll( other.entityKeyIdPairs );
            t0 += other.t0;
            t1 += other.t1;
            c0 += other.c0;
            c1 += other.c1;
        }
    }

    @Override public Boolean aggregate() {
        System.out.println( "THE AVERAGES--------------------------------" );
        System.out.println( "T0" );
        System.out.println( String.valueOf( t0.doubleValue() / c0 ) );
        System.out.println( "T1" );
        System.out.println( String.valueOf( t1.doubleValue() / c1 ) );
        Stopwatch s = Stopwatch.createStarted();
        linkingEntityKeyIdPairs.putAll( entityKeyIdPairs );
        System.out.println("FINALLY");
        System.out.println(String.valueOf( s.elapsed( TimeUnit.MILLISECONDS ) ));
        return true;
    }

    @Override public void setHazelcastInstance( HazelcastInstance hazelcastInstance ) {
        this.linkingEntityKeyIdPairs = hazelcastInstance.getMap( HazelcastMap.LINKING_ENTITY_KEY_ID_PAIRS.name() );
    }

    public UUID getGraphId() {
        return graphId;
    }

    public Map<UUID, UUID> getEntitySetIdsToSyncIds() {
        return entitySetIdsToSyncIds;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;

        BlockingAggregator that = (BlockingAggregator) o;

        if ( !graphId.equals( that.graphId ) )
            return false;
        return entitySetIdsToSyncIds.equals( that.entitySetIdsToSyncIds );
    }

    @Override public int hashCode() {
        int result = graphId.hashCode();
        result = 31 * result + entitySetIdsToSyncIds.hashCode();
        return result;
    }
}
