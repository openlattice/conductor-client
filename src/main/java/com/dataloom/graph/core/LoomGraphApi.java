package com.dataloom.graph.core;

import com.dataloom.data.EntityKey;
import com.dataloom.graph.edge.EdgeKey;
import com.dataloom.graph.core.objects.LoomEdgeKey;
import com.dataloom.graph.core.objects.LoomVertexKey;
import com.datastax.driver.core.ResultSetFuture;
import com.hazelcast.core.ICompletableFuture;

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
    void createVertex( UUID vertexId, EntityKey entityKey, UUID entityTypeId );

    ICompletableFuture<Void> createVertexAsync( UUID vertexId, EntityKey entityKey, UUID entityTypeId );

    LoomVertex getVertex( EntityKey entityKey );
    
    void deleteVertex( UUID vertexId );

    List<ResultSetFuture> deleteVertexAsync( UUID vertexId );

    /*
     * Listing vertices
     */
    Stream<LoomVertexKey> getVerticesOfType( UUID entityTypeId );

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

    void addEdge( EntityKey srcVertexKey, EntityKey dstVertexKey, EntityKey edgeEntityKey );

    ResultSetFuture addEdgeAsync(
            UUID srcVertexId,
            UUID srcVertexEntityTypeId,
            UUID dstVertexId,
            UUID dstVertexEntityTypeId,
            UUID edgeId,
            UUID edgeTypeId );

    ResultSetFuture addEdgeAsync( EntityKey srcVertexKey, EntityKey dstVertexKey, EntityKey edgeEntityKey );

    /**
     * An EdgeKey is the pojo for the primary key of edges table. In the current setting, this is source vertexId,
     * destination vertexId, and the edge syncId.
     *
     * @param key
     * @return
     */
    LoomEdgeKey getEdge( EdgeKey key );

    /**
     * An EdgeSelection restricts the columns in the edges table. In the current setting, it should support restriction
     * of
     * <ul>
     * <li>source UUID</li>
     * <li>destination UUID</li>
     * <li>source type</li>
     * <li>destination type</li>
     * <li>edge type</li>
     * </ul>
     * and combinations of these.
     *
     * @param selection
     * @return
     */
    Iterable<LoomEdgeKey> getEdges( EdgeSelection selection );

    void deleteEdge( EdgeKey edgeKey );

    ResultSetFuture deleteEdgeAsync( EdgeKey edgeKey );

    void deleteEdges( UUID srcId );

    ICompletableFuture<UUID> getVertexIdAsync();
}
