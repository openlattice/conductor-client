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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoomGraph implements LoomGraphApi {

    private static final Logger logger = LoggerFactory.getLogger( LoomGraph.class );

    private final GraphQueryService        gqs;
    private final ListeningExecutorService executor;
    private final IMap<EdgeKey, LoomEdge>  edges;

    // vertex id -> dst type id -> edge type id -> dst entity key id
    //    private final IMap<UUID, Neighborhood> backedges;

    //    private final IMap<UUID, Neighborhood> edges;
    //    // vertex id -> dst type id -> edge type id -> dst entity key id
    //    private final IMap<UUID, Neighborhood> backedges;

    public LoomGraph( ListeningExecutorService executor, GraphQueryService gqs, HazelcastInstance hazelcastInstance ) {
        this.edges = hazelcastInstance.getMap( HazelcastMap.EDGES.name() );
        //        this.backedges = hazelcastInstance.getMap( HazelcastMap.BACKEDGES.name() );
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
        //        edges.evict( srcVertexId );
        //        backedges.evict( dstVertexId );

        EdgeKey key = new EdgeKey( srcVertexId, dstVertexEntityTypeId, edgeEntityTypeId, dstVertexId, edgeEntityId );
        LoomEdge edge = new LoomEdge( key,
                srcVertexEntityTypeId,
                srcVertexEntitySetId,
                srcVertexEntitySyncId,
                dstVertexEntitySetId,
                dstVertexEntitySyncId,
                edgeEntitySetId );

        return new ListenableHazelcastFuture<>( edges.setAsync( key, edge ) );
        //        return gqs.putEdgeAsync( srcVertexId,
        //                srcVertexEntityTypeId,
        //                srcVertexEntitySetId,
        //                dstVertexId,
        //                dstVertexEntityTypeId,
        //                dstVertexEntitySetId,
        //                edgeEntityId,
        //                edgeEntityTypeId,
        //                edgeEntitySetId );
    }

    @Override
    public void deleteVertex( UUID vertexId ) {
        StreamUtil.getUninterruptibly( deleteVertexAsync( vertexId ) );
    }

    @Override
    public ListenableFuture deleteVertexAsync( UUID vertex ) {
        // TODO: Implement delete for neighborhoods
        return executor.submit( () -> edges.removeAll( Predicates.equal( "srcEntityKeyId", vertex ) ) );
        //        return gqs
        //                .getEdges( ImmutableMap.of( CommonColumns.SRC_ENTITY_KEY_ID, ImmutableSet.of( vertex ) ) )
        //                .map( LoomEdge::getKey )
        //                .map( this::deleteEdgeAsync )
        //                .forEach( StreamUtil::getUninterruptibly );
    }

    @Override
    public LoomEdge getEdge( EdgeKey key ) {
        return gqs.getEdge( key );
    }

    @Override
    public Stream<LoomEdge> getEdges( Map<CommonColumns, Set<UUID>> edgeSelection ) {
        return gqs.getFromEdgesTable( edgeSelection );
    }

    @Override
    public void deleteEdge( EdgeKey key ) {
        StreamUtil.getUninterruptibly( deleteEdgeAsync( key ) );
    }

    @Override
    public ListenableFuture deleteEdgeAsync( EdgeKey edgeKey ) {
        return executor.submit( () -> edges.delete( edgeKey ) );

        //        edges.removeAll( edge( edgeKey ) );
        //        edges.evict( edgeKey.getSrcEntityKeyId() );
        //        backedges.evict( edgeKey.getDstEntityKeyId() );
        //        return gqs.deleteEdgeAsync( getEdge( edgeKey ) );
    }

    @Override
    public void deleteEdges( UUID srcId ) {
        gqs.deleteEdgesBySrcId( srcId );
    }

    @Override
    @Timed
    public Stream<LoomEdge> getEdgesAndNeighborsForVertex( UUID vertexId ) {
        //TODO: This should be fine as long as neighborhood don't get too large.
        //return edges.values( Predicates.equal( "srcEntityKeyId", vertexId ) ).stream();
        return edges.values( Predicates.equal( "srcEntityKeyId", vertexId ) ).stream();
        //        return gqs
        //                .getEdges( ImmutableMap.of( CommonColumns.SRC_ENTITY_KEY_ID, ImmutableSet.of( vertexId ) ) );
    }

    @Override
    @Deprecated
    public ResultSetFuture getEdgeCount(
            UUID vertexId,
            UUID associationTypeId,
            Set<UUID> neighborTypeIds,
            boolean vertexIsSrc ) {

        return gqs.getNeighborEdgeCountAsync( vertexId, associationTypeId, neighborTypeIds, vertexIsSrc );
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

    @Override
    public int getHazelcastEdgeCount(
            UUID vertexId,
            UUID associationTypeId,
            Set<UUID> neighborTypeIds,
            boolean vertexIsSrc ) {
        //        if ( vertexIsSrc ) {
        //            return (Integer) edges.executeOnKey( vertexId,
        //                    new EdgeCountEntryProcessor( associationTypeId, neighborTypeIds ) );
        //        }
        //
        //        return (Integer) backedges
        //                .executeOnKey( vertexId, new EdgeCountEntryProcessor( associationTypeId, neighborTypeIds ) );
        return 0;
    }


    public static Predicate edgesMatching(
            UUID entitySetId,
            UUID syncId,
            SetMultimap<UUID, UUID> srcFilters,
            SetMultimap<UUID, UUID> dstFilters ) {
        /*
         * No need to execute on back edge map  we are looking for items in specified entity set that have incoming edges
         * of a given type from a given destination type. That means srcType =
         * We are looking for anything of that type id to the src entity set -> dst where
         */
        return Predicates.or(
                Stream.concat(dstFilters.entries().stream()
                                .map( dstFilter -> Predicates.and(
                                        Predicates.equal( "dstSetId", entitySetId ),
                                        Predicates.equal( "dstSyncId", syncId ),
                                        Predicates.equal( "edgeTypeId", dstFilter.getKey() ),
                                        Predicates.equal( "srcTypeId", dstFilter.getValue() ) ) ) ,
                        srcFilters.entries().stream()
                                .map( srcFilter -> Predicates.and(
                                        Predicates.equal( "srcSetId", entitySetId ),
                                        Predicates.equal( "srcSyncId", syncId ),
                                        Predicates.equal( "edgeTypeId", srcFilter.getKey() ),
                                        Predicates.equal( "dstTypeId", srcFilter.getValue() ) ) ) )
                        .toArray( Predicate[]::new ) );
        //Predicate p = Predicates.or( Predicates.equal("srcSetId", entitySetId), Predicates.equal(  ) )

    }

    public static Predicate anyOf( Set<UUID> dstTypeIds ) {
        return Predicates.in( "dstTypeId", dstTypeIds.toArray( new UUID[ 0 ] ) );
        //        return Predicates.or( dstTypeIds
        //                .stream()
        //                .map( dstTypeId -> Predicates.equal( "dstTypeId", dstTypeId ) )
        //                .toArray( Predicate[]::new ) );
    }

}
