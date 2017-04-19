package com.dataloom.graph.core;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;

import com.dataloom.data.EntityKey;
import com.dataloom.graph.edge.EdgeKey;
import com.dataloom.graph.core.objects.LoomEdgeKey;
import com.dataloom.graph.core.objects.LoomVertexKey;
import com.dataloom.graph.edge.EdgeKey;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.ListenableFuture;
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
    void createVertex( UUID vertexId, EntityKey entityKey );

    ResultSetFuture createVertexAsync( UUID vertexId, EntityKey entityKey );

    UUID getVertexId( EntityKey entityKey );

    void deleteVertex( UUID vertexId );

    List<ResultSetFuture> deleteVertexAsync( UUID vertexId );

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

    void deleteEdge( EdgeKey edgeKey );

    ResultSetFuture deleteEdgeAsync( EdgeKey edgeKey );

    void deleteEdges( UUID srcId );

    Pair<List<LoomEdgeKey>, List<LoomEdgeKey>> getEdgesAndNeighborsForVertex( UUID vertexId );
}
