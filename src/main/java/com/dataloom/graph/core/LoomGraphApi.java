package com.dataloom.graph.core;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import com.dataloom.graph.edge.EdgeKey;
import com.dataloom.graph.edge.LoomEdge;
import com.datastax.driver.core.ResultSetFuture;
import com.kryptnostic.datastore.cassandra.CommonColumns;

/**
 * Graph Object supporting CRUD operations of vertices and edges to the graph.
 *
 * @author Ho Chung Siu
 */
public interface LoomGraphApi {

    /*
     * CRUD operations of vertices
     */
    void createVertex( UUID vertexId );

    ResultSetFuture createVertexAsync( UUID vertexId );

    void deleteVertex( UUID vertexId );

    Stream<ResultSetFuture> deleteVertexAsync( UUID vertexId );

    /*
     * CRUD operations of edges
     */
    void addEdge(
            UUID srcVertexId,
            UUID srcVertexEntityTypeId,
            UUID srcVertexEntitySetId,
            UUID dstVertexId,
            UUID dstVertexEntityTypeId,
            UUID dstVertexEntitySetId,
            UUID edgeId,
            UUID edgeTypeId,
            UUID edgeEntitySetId );

    List<ResultSetFuture> addEdgeAsync(
            UUID srcVertexId,
            UUID srcVertexEntityTypeId,
            UUID srcVertexEntitySetId,
            UUID dstVertexId,
            UUID dstVertexEntityTypeId,
            UUID dstVertexEntitySetId,
            UUID edgeId,
            UUID edgeTypeId,
            UUID edgeEntitySetId );

    /**
     * An EdgeKey is the pojo for the primary key of edges table. In the current setting, this is source vertexId,
     * destination vertexId, and the edge syncId.
     *
     * @param key
     * @return
     */
    LoomEdge getEdge( EdgeKey key );

    /**
     * Select edges where the column values must lie in the Set &lt; UUID &gt; specified. 
     * @param edgeSelection
     * @return
     */
    Stream<LoomEdge> getEdges( Map<CommonColumns, Set<UUID>> edgeSelection );

    void deleteEdge( EdgeKey edgeKey );

    List<ResultSetFuture> deleteEdgeAsync( EdgeKey edgeKey );

    void deleteEdges( UUID srcId );

    Stream<LoomEdge> getEdgesAndNeighborsForVertex( UUID vertexId );

    ResultSetFuture getEdgeCount(
            UUID vertexId,
            UUID associationTypeId,
            Set<UUID> neighborTypeIds,
            boolean vertexIsSrc );

}
