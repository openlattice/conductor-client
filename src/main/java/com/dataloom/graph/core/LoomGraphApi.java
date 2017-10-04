package com.dataloom.graph.core;

import com.dataloom.data.analytics.IncrementableWeightId;
import com.dataloom.graph.edge.EdgeKey;
import com.dataloom.graph.edge.LoomEdge;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.ListenableFuture;
import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.query.Predicate;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * Graph Object supporting CRUD operations of vertices and edges to the graph.
 *
 * @author Ho Chung Siu
 */
public interface LoomGraphApi {

//    /*
//     * CRUD operations of vertices
//     */
//    void createVertex( UUID vertexId );
//
//    ResultSetFuture createVertexAsync( UUID vertexId );
//
    void deleteVertex( UUID vertexId );

    ListenableFuture deleteVertexAsync( UUID vertexId );

    /*
     * CRUD operations of edges
     */
    void addEdge(
            UUID srcVertexId,
            UUID srcVertexEntityTypeId,
            UUID srcVertexEntitySetId,
            UUID srcVertexEntitySyncId,
            UUID dstVertexId,
            UUID dstVertexEntityTypeId,
            UUID dstVertexEntitySetId,
            UUID dstVertexEntitySyncId,
            UUID edgeId,
            UUID edgeTypeId,
            UUID edgeEntitySetId );

    ListenableFuture addEdgeAsync(
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
            UUID edgeEntitySetId );

    /**
     * An EdgeKey is the pojo for the primary key of edges table. In the current setting, this is source vertexId,
     * destination vertexId, and the edge syncId.
     */
    LoomEdge getEdge( EdgeKey key );

    Void submitAggregator( Aggregator<Entry<EdgeKey, LoomEdge>, Void> agg, Predicate p );

    void deleteEdge( EdgeKey edgeKey );

    ListenableFuture deleteEdgeAsync( EdgeKey edgeKey );

    void deleteEdges( UUID srcId );

    Stream<LoomEdge> getEdgesAndNeighborsForVertex( UUID vertexId );
    
    Stream<LoomEdge> getEdgesAndNeighborsForVertices( Set<UUID> vertexIds );

    IncrementableWeightId[] computeGraphAggregation(
            int limit,
            UUID entitySetId,
            UUID syncId,
            SetMultimap<UUID, UUID> srcFilters,
            SetMultimap<UUID, UUID> dstFilters );

}