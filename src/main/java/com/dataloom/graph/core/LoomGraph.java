package com.dataloom.graph.core;

import com.dataloom.data.EntityKey;
import com.dataloom.graph.LoomElement;
import com.dataloom.graph.core.objects.LoomEdgeKey;
import com.dataloom.graph.core.objects.LoomVertexKey;
import com.dataloom.graph.edge.EdgeKey;
import com.dataloom.graph.vertex.NeighborhoodSelection;
import com.dataloom.hazelcast.HazelcastMap;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.collect.ImmutableSet;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.kryptnostic.datastore.services.EdmService;
import com.kryptnostic.datastore.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LoomGraph implements LoomGraphApi {

    private static final Logger logger = LoggerFactory.getLogger( LoomGraph.class );

    private final EdmService                   edm;
    private final GraphQueryService            gqs;
    private final IMap<EntityKey, LoomElement> vertices;
    private final IMap<EntityKey, LoomElement> edges;

    public LoomGraph( EdmService edm, GraphQueryService gqs, HazelcastInstance hazelcastInstance ) {
        this.gqs = gqs;
        this.edm = edm;
        this.vertices = hazelcastInstance.getMap( HazelcastMap.VERTICES.name() );
        this.edges = hazelcastInstance.getMap( HazelcastMap.ENTITY_EDGES.name() );
    }

    @Override
    public void createVertex( UUID vertexId, EntityKey entityKey, UUID entityTypeId ) {
        vertices.set( entityKey, new LoomElement( vertexId, entityTypeId ) );
    }

    @Override
    public ICompletableFuture<Void> createVertexAsync(
            UUID vertexId, EntityKey entityKey, UUID entityTypeId ) {
        return vertices.setAsync( entityKey, new LoomElement( vertexId, entityTypeId ) );
    }

    @Override
    public UUID getVertexId( EntityKey entityKey ) {
        return Util.getSafely( vertices, entityKey ).getId();
    }

    @Override
    public Stream<LoomVertexKey> getVerticesOfType( UUID entityTypeId ) {
        return gqs.getVerticesOfType( entityTypeId );
    }

    @Override
    public void addEdge(
            UUID srcVertexId,
            UUID srcVertexEntityTypeId,
            UUID dstVertexId,
            UUID dstVertexEntityTypeId,
            UUID edgeEntityId,
            UUID edgeEntityTypeId ) {
        addEdgeAsync( srcVertexId,
                srcVertexEntityTypeId,
                dstVertexId,
                dstVertexEntityTypeId,
                edgeEntityId,
                edgeEntityTypeId ).getUninterruptibly();
    }

    @Override
    public ResultSetFuture addEdgeAsync(
            UUID srcVertexId,
            UUID srcVertexEntityTypeId,
            UUID dstVertexId,
            UUID dstVertexEntityTypeId,
            UUID edgeEntityId,
            UUID edgeEntityTypeId ) {
        return gqs.putEdgeAsync( srcVertexId,
                srcVertexEntityTypeId,
                dstVertexId,
                dstVertexEntityTypeId,
                edgeEntityId,
                edgeEntityTypeId );
    }

    @Override
    public void deleteVertex( UUID vertexId ) {
        deleteVertexAsync( vertexId ).forEach( ResultSetFuture::getUninterruptibly );
    }

    @Override
    public List<ResultSetFuture> deleteVertexAsync( UUID vertex ) {
        NeighborhoodSelection ns = new NeighborhoodSelection( vertex, ImmutableSet.of(), ImmutableSet.of() );
        Stream<EdgeKey> edgesKey = gqs.getNeighborhood( ns );
        return edgesKey.map( this::deleteEdgeAsync ).collect( Collectors.toList() );
    }

    @Override
    public void addEdge( EntityKey src, EntityKey dst, EntityKey edgeLabel ) {
        addEdgeAsync( src, dst, edgeLabel ).getUninterruptibly();
    }

    @Override
    public ResultSetFuture addEdgeAsync( EntityKey src, EntityKey dst, EntityKey edgeKey ) {
        LoomElement srcVertex = Util.getSafely( vertices, src );
        LoomElement dstVertex = Util.getSafely( vertices, dst );
        LoomElement edge = Util.getSafely( edges, edgeKey );
        return gqs.putEdgeAsync( srcVertex.getId(),
                srcVertex.getTypeId(),
                dstVertex.getId(),
                dstVertex.getTypeId(),
                edge.getId(),
                edge.getTypeId() );
    }

    @Override
    public LoomEdgeKey getEdge( EdgeKey key ) {
        return gqs.getEdge( key );
    }

    @Override
    public Iterable<LoomEdgeKey> getEdges( EdgeSelection selection ) {
        return gqs.getEdges( selection );
    }

    @Override
    public void deleteEdge( EdgeKey key ) {
        gqs.deleteEdge( key );
    }

    @Override
    public ResultSetFuture deleteEdgeAsync( EdgeKey edgeKey ) {
        return gqs.deleteEdgeAsync( edgeKey );
    }

    @Override
    public void deleteEdges( UUID srcId ) {
        gqs.deleteEdgesBySrcId( srcId );
    }

}
