package com.dataloom.graph.core;

import com.dataloom.graph.edge.LoomEdge;
import com.dataloom.graph.edge.EdgeKey;
import com.datastax.driver.core.ResultSetFuture;

import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

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
            UUID dstVertexId,
            UUID dstVertexEntityTypeId,
            UUID edgeId,
            UUID edgeTypeId );

    List<ResultSetFuture> addEdgeAsync(
            UUID srcVertexId,
            UUID srcVertexEntityTypeId,
            UUID dstVertexId,
            UUID dstVertexEntityTypeId,
            UUID edgeId,
            UUID edgeTypeId );

    /**
     * An EdgeKey is the pojo for the primary key of edges table. In the current setting, this is source vertexId,
     * destination vertexId, and the edge syncId.
     *
     * @param key
     * @return
     */
    LoomEdge getEdge( EdgeKey key );

    void deleteEdge( EdgeKey edgeKey );

    ResultSetFuture deleteEdgeAsync( EdgeKey edgeKey );

    void deleteEdges( UUID srcId );

    Stream<LoomEdge> getEdgesAndNeighborsForVertex( UUID vertexId );
}
