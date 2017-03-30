package com.dataloom.graph.core;

import java.util.UUID;

import com.dataloom.data.EntityKey;
import com.dataloom.graph.core.objects.EdgeKey;
import com.dataloom.graph.core.objects.LoomEdge;
import com.dataloom.graph.core.objects.LoomVertex;

/**
 * Graph Object supporting CRUD operations of vertices and edges to the graph. Top Utilizers queries should be covered
 * by {@link #getEdges(EdgeSelection) } endpoint.
 * 
 * @author Ho Chung Siu
 *
 */
public interface LoomGraphApi {

    UUID getId();
    /*
     * CRUD operations of vertices
     */
    LoomVertex getOrCreateVertex( EntityKey entityKey );

    LoomVertex getVertex( UUID vertexId );

    void deleteVertex( UUID vertexId );

    /*
     * CRUD operations of edges
     */
    LoomEdge addEdge( LoomVertex src, LoomVertex dst, EntityKey edgeLabel );

    /**
     * An EdgeKey is the pojo for the primary key of edges table. In the current setting, this is source vertexId,
     * destination vertexId, and timeuuid for time written.
     * 
     * @param key
     * @return
     */
    LoomEdge getEdge( EdgeKey edgeKey );

    /**
     * An EdgeSelection restricts the columns in the edges table. In the current setting, it should support restriction of
     * <ul>
     * <li>source UUID</li>
     * <li>destination UUID</li>
     * <li>source type</li>
     * <li>destination type</li>
     * <li>edge type</li>
     * </ul>
     * and combinations of these.
     * @param selection
     * @return
     */
    Iterable<LoomEdge> getEdges( EdgeSelection selection );

    void deleteEdge( EdgeKey edgeKey );

    void deleteEdges( UUID srcId );

}
