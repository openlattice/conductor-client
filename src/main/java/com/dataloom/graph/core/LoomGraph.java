package com.dataloom.graph.core;

import com.codahale.metrics.annotation.Timed;
import com.dataloom.data.analytics.IncrementableWeightId;
import com.dataloom.graph.aggregators.GraphCount;
import com.dataloom.graph.edge.EdgeKey;
import com.dataloom.graph.edge.LoomEdge;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.hazelcast.ListenableHazelcastFuture;
import com.dataloom.streams.StreamUtil;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

public class LoomGraph implements LoomGraphApi {

    private final ListeningExecutorService executor;
    private final IMap<EdgeKey, LoomEdge>  edges;

    // vertex id -> dst type id -> edge type id -> dst entity key id
    // private final IMap<UUID, Neighborhood> backedges;

    // private final IMap<UUID, Neighborhood> edges;
    // // vertex id -> dst type id -> edge type id -> dst entity key id
    // private final IMap<UUID, Neighborhood> backedges;

    public LoomGraph( ListeningExecutorService executor, HazelcastInstance hazelcastInstance ) {
        this.edges = hazelcastInstance.getMap( HazelcastMap.EDGES.name() );
        this.executor = executor;
    }

    @Override
    public void addEdge(
            UUID srcVertexId,
            UUID srcVertexEntityTypeId,
            UUID srcVertexEntitySetId,
            UUID srcVertexEntitySyncId,
            UUID dstVertexId,
            UUID dstVertexEntityTypeId,
            UUID dstVertexEntitySetId,
            UUID dstVertexEntitySyncId,
            UUID edgeEntityId,
            UUID edgeEntityTypeId,
            UUID edgeEntitySetId ) {
        StreamUtil.getUninterruptibly( addEdgeAsync( srcVertexId,
                srcVertexEntityTypeId,
                srcVertexEntitySetId,
                srcVertexEntitySyncId,
                dstVertexId,
                dstVertexEntityTypeId,
                dstVertexEntitySetId,
                dstVertexEntitySyncId,
                edgeEntityId,
                edgeEntityTypeId,
                edgeEntitySetId ) );
    }

    @Override
    public ListenableFuture<Void> addEdgeAsync(
            UUID srcVertexId,
            UUID srcVertexEntityTypeId,
            UUID srcVertexEntitySetId,
            UUID srcVertexEntitySyncId,
            UUID dstVertexId,
            UUID dstVertexEntityTypeId,
            UUID dstVertexEntitySetId,
            UUID dstVertexEntitySyncId,
            UUID edgeEntityId,
            UUID edgeEntityTypeId,
            UUID edgeEntitySetId ) {

        EdgeKey key = new EdgeKey( srcVertexId, dstVertexEntityTypeId, edgeEntityTypeId, dstVertexId, edgeEntityId );
        LoomEdge edge = new LoomEdge(
                key,
                srcVertexEntityTypeId,
                srcVertexEntitySetId,
                srcVertexEntitySyncId,
                dstVertexEntitySetId,
                dstVertexEntitySyncId,
                edgeEntitySetId );

        return new ListenableHazelcastFuture<>( edges.setAsync( key, edge ) );
    }

    @Override
    public void deleteVertex( UUID vertexId ) {
        StreamUtil.getUninterruptibly( deleteVertexAsync( vertexId ) );
    }

    @Override
    public ListenableFuture deleteVertexAsync( UUID vertex ) {
        // TODO: Implement delete for neighborhoods
        return executor.submit( () -> edges.removeAll( neighborhood( vertex ) ) );
    }

    @Override
    public LoomEdge getEdge( EdgeKey key ) {
        return edges.get( key );
    }

    @Override
    public Void submitAggregator( Aggregator<Entry<EdgeKey, LoomEdge>, Void> agg, Predicate p ) {
        return edges.aggregate( agg, p );
    }

    @Override
    public void deleteEdge( EdgeKey key ) {
        StreamUtil.getUninterruptibly( deleteEdgeAsync( key ) );
    }

    @Override
    public ListenableFuture deleteEdgeAsync( EdgeKey edgeKey ) {
        return executor.submit( () -> edges.delete( edgeKey ) );
    }

    @Override
    public void deleteEdges( UUID srcId ) {
        edges.removeAll( Predicates.equal( "srcEntityKeyId", srcId ) );
    }

    @Override
    @Timed
    public Stream<LoomEdge> getEdgesAndNeighborsForVertex( UUID vertexId ) {
        return edges.values( neighborhood( vertexId ) ).stream();
    }

    @Override
    @Timed
    public Stream<LoomEdge> getEdgesAndNeighborsForVertices( Set<UUID> vertexIds ) {
        return edges.values( Predicates.or( Predicates.in( "srcEntityKeyId", vertexIds.toArray( new UUID[] {} ) ),
                Predicates.in( "dstEntityKeyId", vertexIds.toArray( new UUID[] {} ) ) ) ).stream();
    }

    @Override
    @Timed
    public IncrementableWeightId[] computeGraphAggregation(
            int limit,
            UUID entitySetId,
            UUID syncId,
            SetMultimap<UUID, UUID> srcFilters,
            SetMultimap<UUID, UUID> dstFilters ) {
        Predicate p = edgesMatching( entitySetId, syncId, srcFilters, dstFilters );
        return this.edges.aggregate( new GraphCount( limit, entitySetId ), p );
    }

    static Predicate neighborhood( UUID entityKeyId ) {
        return Predicates.or(
                Predicates.equal( "dstEntityKeyId", entityKeyId ),
                Predicates.equal( "srcEntityKeyId", entityKeyId ) );
    }

    public static Predicate edgesMatching(
            UUID entitySetId,
            UUID syncId,
            SetMultimap<UUID, UUID> srcFilters,
            SetMultimap<UUID, UUID> dstFilters ) {
        /*
         * No need to execute on back edge map we are looking for items in specified entity set that have incoming edges
         * of a given type from a given destination type. That means srcType = We are looking for anything of that type
         * id to the src entity set -> dst where
         */
        return Predicates.or(
                Stream.concat( dstFilters.entries().stream()
                                .map( dstFilter -> Predicates.and(
                                        Predicates.equal( "dstSetId", entitySetId ),
                                        Predicates.equal( "dstSyncId", syncId ),
                                        Predicates.equal( "edgeTypeId", dstFilter.getKey() ),
                                        Predicates.equal( "srcTypeId", dstFilter.getValue() ) ) ),
                        srcFilters.entries().stream()
                                .map( srcFilter -> Predicates.and(
                                        Predicates.equal( "srcSetId", entitySetId ),
                                        Predicates.equal( "srcSyncId", syncId ),
                                        Predicates.equal( "edgeTypeId", srcFilter.getKey() ),
                                        Predicates.equal( "dstTypeId", srcFilter.getValue() ) ) ) )
                        .toArray( Predicate[]::new ) );

    }

}
