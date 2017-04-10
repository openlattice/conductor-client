package com.dataloom.graph.core;

import java.util.Map;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;

import com.dataloom.data.EntityKey;
import com.dataloom.graph.core.objects.EdgeKey;
import com.dataloom.graph.core.objects.EdgeSelection;
import com.dataloom.graph.core.objects.LoomEdge;
import com.dataloom.graph.core.objects.LoomVertex;
import com.google.common.collect.SetMultimap;

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

    LoomVertexFuture createVertexAsync( EntityKey entityKey );

    LoomVertex getVertexById( UUID vertexId );

    LoomVertex getVertexByEntityKey( EntityKey entityKey );

    void deleteVertex( UUID vertexId );
    
    LoomVertexFuture deleteVertexAsync( UUID vertexId );

    /*
     * CRUD operations of edges
     */
    LoomEdge addEdge( LoomVertex src, LoomVertex dst, EntityKey edgeLabel );

    LoomEdgeFuture addEdgeAsync( LoomVertex src, LoomVertex dst, EntityKey edgeLabel );

    LoomEdge addEdge( EntityKey src, EntityKey dst, EntityKey label);

    /**
     * An EdgeKey is the pojo for the primary key of edges table. In the current setting, this is source vertexId,
     * destination vertexId, and the edge syncId.
     * 
     * @param key
     * @return
     */
    LoomEdge getEdge( EdgeKey key );

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

    LoomEdgeFuture deleteEdgeAsync( EdgeKey edgeKey );
    
    void deleteEdges( UUID srcId );

}
