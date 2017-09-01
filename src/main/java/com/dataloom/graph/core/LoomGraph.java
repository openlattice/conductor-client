package com.dataloom.graph.core;

import com.codahale.metrics.annotation.Timed;
import com.dataloom.data.analytics.IncrementableWeightId;
import com.dataloom.graph.aggregators.GraphCount;
import com.dataloom.graph.edge.EdgeKey;
import com.dataloom.graph.edge.LoomEdge;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.hazelcast.ListenableHazelcastFuture;
import com.dataloom.streams.StreamUtil;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

public class LoomGraph implements LoomGraphApi {

    private final GraphQueryService        gqs;
    private final ListeningExecutorService executor;
    private final IMap<EdgeKey, LoomEdge>  edges;

    // vertex id -> dst type id -> edge type id -> dst entity key id
    // private final IMap<UUID, Neighborhood> backedges;

    // private final IMap<UUID, Neighborhood> edges;
    // // vertex id -> dst type id -> edge type id -> dst entity key id
    // private final IMap<UUID, Neighborhood> backedges;

    public LoomGraph( ListeningExecutorService executor, GraphQueryService gqs, HazelcastInstance hazelcastInstance ) {
        this.edges = hazelcastInstance.getMap( HazelcastMap.EDGES.name() );
        this.executor = executor;
        this.gqs = gqs;
    }

    @Override
    public void createVertex( UUID vertexId ) {
        createVertexAsync( vertexId ).getUninterruptibly();
    }

    @Override
    public ResultSetFuture createVertexAsync( UUID vertexId ) {
        return gqs.createVertexAsync( vertexId );
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
        return executor.submit( () -> edges.removeAll( Predicates.equal( "srcEntityKeyId", vertex ) ) );
    }

    @Override
    public LoomEdge getEdge( EdgeKey key ) {
        return gqs.getEdge( key );
    }

    @Override
    @Timed
    public Stream<LoomEdge> getEdges( Map<CommonColumns, Set<UUID>> edgeSelection ) {
        // TODO: This is for linking will fix later
        // return edges.values( Predicates.or(
        // Predicates.equal( "dstEntityKeyId", vertexId ),
        // Predicates.equal( "srcEntityKeyId", vertexId ) ) ).stream();
        return gqs.getFromEdgesTable( edgeSelection );
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
        gqs.deleteEdgesBySrcId( srcId );
    }

    @Override
    @Timed
    public Stream<LoomEdge> getEdgesAndNeighborsForVertex( UUID vertexId ) {
        return edges.values( Predicates.or(
                Predicates.equal( "dstEntityKeyId", vertexId ),
                Predicates.equal( "srcEntityKeyId", vertexId ) ) ).stream();
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
