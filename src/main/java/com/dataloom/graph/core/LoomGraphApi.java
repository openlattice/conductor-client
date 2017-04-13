package com.dataloom.graph.core;

import java.util.List;
import java.util.UUID;

import com.dataloom.data.EntityKey;
import com.dataloom.graph.core.objects.EdgeKey;
import com.dataloom.graph.core.objects.EdgeSelection;
import com.dataloom.graph.core.objects.LoomEdge;
import com.dataloom.graph.core.objects.LoomVertex;
import com.dataloom.graph.core.objects.LoomVertexFuture;
import com.datastax.driver.core.ResultSetFuture;

/**
 * Graph Object supporting CRUD operations of vertices and edges to the graph.
 * 
 * @author Ho Chung Siu
 *
 */
public interface LoomGraphApi {

    /*
     * CRUD operations of vertices
     */
    LoomVertex getOrCreateVertex( EntityKey entityKey );

    LoomVertexFuture getOrCreateVertexAsync( EntityKey entityKey );

    LoomVertex getVertexById( UUID vertexId );

    LoomVertex getVertexByEntityKey( EntityKey entityKey );

    void deleteVertex( UUID vertexId );

    List<ResultSetFuture> deleteVertexAsync( UUID vertexId );

    /*
     * CRUD operations of edges
     */
    void addEdge( LoomVertex src, LoomVertex dst, EntityKey edgeLabel );

    void addEdge( EntityKey src, EntityKey dst, EntityKey edgeLabel );

    ResultSetFuture addEdgeAsync( LoomVertex src, LoomVertex dst, EntityKey edgeLabel );

    ResultSetFuture addEdgeAsync( EntityKey src, EntityKey dst, EntityKey edgeLabel );

    /**
     * An EdgeKey is the pojo for the primary key of edges table. In the current setting, this is source vertexId,
     * destination vertexId, and the edge syncId.
     * 
     * @param key
     * @return
     */
    LoomEdge getEdge( EdgeKey key );

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
    Iterable<LoomEdge> getEdges( EdgeSelection selection );

    void deleteEdge( EdgeKey edgeKey );

    ResultSetFuture deleteEdgeAsync( EdgeKey edgeKey );

    void deleteEdges( UUID srcId );

}
