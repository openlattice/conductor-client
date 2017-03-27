package com.dataloom.graph.core;

import java.util.UUID;

import com.dataloom.data.EntityKey;
import com.dataloom.graph.core.impl.EdgeKey;
import com.dataloom.graph.core.impl.Label;
import com.dataloom.graph.core.impl.LoomEdge;
import com.dataloom.graph.core.impl.LoomVertex;

/**
 * Graph Object supporting CRUD operations of vertices and edges to the graph. Top Utilizers queries should be covered
 * by {@link #getEdges(EdgeSelection) } endpoint.
 * 
 * @author Ho Chung Siu
 *
 */
public interface LoomGraph {

    /*
     * CRUD operations of vertices
     */
    /**
     * Initialize a vertex with specified label.
     * <p>
     * Note: {@link Label} corresponds to identifier (id) of the vertex/edge, as well as any extra information.
     * (EntityKey in our setting)
     * </p>
     * 
     * @param vertexLabel
     * @return
     */
    public LoomVertex addVertex( Label vertexLabel );

    /**
     * Helper method of adding an vertex, by implicitly registering the label for the entity key (vertex label)
     * 
     * @param entityKey
     * @return
     */
    public LoomVertex addVertex( EntityKey entityKey );

    public LoomVertex getVertex( UUID id );

    public void deleteVertex( UUID id );

    /*
     * CRUD operations of edges
     */
    public LoomEdge addEdge( LoomVertex src, LoomVertex dst, Label edgeLabel );

    /**
     * Helper method of adding an edge, by implicitly registering the label for the entity key (edge label)
     * 
     * @param entityKey
     * @return
     */
    public LoomEdge addEdge( LoomVertex src, LoomVertex dst, EntityKey edgeLabel );

    /**
     * An EdgeKey is the pojo for the primary key of edges table. In the current setting, this is source vertexId,
     * destination vertexId, and timeuuid for time written.
     * 
     * @param key
     * @return
     */
    public LoomEdge getEdge( EdgeKey key );

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
    public Iterable<LoomEdge> getEdges( EdgeSelection selection );

    public void deleteEdge( EdgeKey key );

    public void deleteEdges( UUID srcId );

}
