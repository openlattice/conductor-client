package com.dataloom.graph.core;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.data.EntityKey;
import com.dataloom.graph.core.objects.EdgeKey;
import com.dataloom.graph.core.objects.EdgeLabel;
import com.dataloom.graph.core.objects.EdgeSelection;
import com.dataloom.graph.core.objects.GraphWrappedEdgeKey;
import com.dataloom.graph.core.objects.GraphWrappedEntityKey;
import com.dataloom.graph.core.objects.GraphWrappedVertexId;
import com.dataloom.graph.core.objects.LoomEdge;
import com.dataloom.graph.core.objects.LoomVertex;
import com.dataloom.graph.core.objects.VertexLabel;
import com.dataloom.hazelcast.HazelcastMap;
import com.google.common.base.Optional;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public class LoomGraph implements LoomGraphApi {

    private static final Logger                           logger           = LoggerFactory.getLogger( LoomGraph.class );
    private static UUID                                   DEFAULT_GRAPH_ID = new UUID( 0, 0 );
    private UUID                                          graphId;

    private static IMap<GraphWrappedVertexId, LoomVertex> vertices;
    private static IMap<GraphWrappedEntityKey, UUID>      verticesLookup;

    private static IMap<GraphWrappedEdgeKey, LoomEdge>    edges;
    private static GraphQueryService                      gqs;

    public static void init( HazelcastInstance hazelcastInstance, GraphQueryService gqs ) {
        LoomGraph.vertices = hazelcastInstance.getMap( HazelcastMap.VERTICES.name() );
        LoomGraph.verticesLookup = hazelcastInstance.getMap( HazelcastMap.VERTICES_LOOKUP.name() );
        LoomGraph.edges = hazelcastInstance.getMap( HazelcastMap.EDGES.name() );

        LoomGraph.gqs = gqs;
    }

    public LoomGraph() {
        this( DEFAULT_GRAPH_ID );
    }

    public LoomGraph( UUID graphId ) {
        this.graphId = graphId;
    }

    @Override
    public UUID getId() {
        return graphId;
    }

    @Override
    public LoomVertex getOrCreateVertex( EntityKey entityKey ) {
        // Put if absent in verticesLookup
        // Generate random UUID, put if absent in vertices until it succeeds
        GraphWrappedEntityKey lookupKey = new GraphWrappedEntityKey( graphId, entityKey );

        while ( true ) {
            UUID proposedId = UUID.randomUUID();
            UUID currentId = verticesLookup.putIfAbsent( lookupKey, proposedId );
            if ( currentId != null ) {
                return vertices.get( new GraphWrappedVertexId( graphId, currentId ) );
            } else {
                GraphWrappedVertexId key = new GraphWrappedVertexId( graphId, proposedId );
                LoomVertex vertex = new LoomVertex( graphId, proposedId, new VertexLabel( entityKey ) );
                if ( vertices.putIfAbsent( key, vertex ) == null ) {
                    return vertex;
                } else {
                    // creation failed. Rollback the insertion to verticesLookup, and restart the UUID generation
                    // process again.
                    verticesLookup.remove( lookupKey );
                }
            }
        }
    }

    @Override
    public LoomVertex getVertex( UUID vertexId ) {
        return vertices.get( new GraphWrappedVertexId( graphId, vertexId ) );
    }

    @Override
    public void deleteVertex( UUID vertexId ) {
        // TODO delete all the incident edges
    }

    @Override
    public LoomEdge addEdge( LoomVertex src, LoomVertex dst, EntityKey edgeLabel ) {
        EdgeKey key = new EdgeKey( src.getKey(), dst.getKey() );
        EdgeLabel label = new EdgeLabel(
                edgeLabel,
                src.getLabel().getReference().getEntitySetId(),
                dst.getLabel().getReference().getEntitySetId() );
        LoomEdge edge = new LoomEdge( graphId, key, label );
        if ( edges.putIfAbsent( new GraphWrappedEdgeKey( graphId, key ), edge ) == null ) {
            return edge;
        } else {
            logger.debug( "Edge creation failed: edge key was already in use." );
            return null;
        }
    }

    @Override
    public LoomEdge addEdge( EntityKey src, EntityKey dst, EntityKey label ) {
        LoomVertex srcVertex = getOrCreateVertex( src );
        LoomVertex dstVertex = getOrCreateVertex( dst );
        return addEdge( srcVertex, dstVertex, label );
    }

    @Override
    public LoomEdge getEdge( EdgeKey key ) {
        return edges.get( new GraphWrappedEdgeKey( graphId, key ) );
    }

    @Override
    public Iterable<LoomEdge> getEdges( EdgeSelection selection ) {
        return gqs.getEdges( graphId, selection );
    }

    @Override
    public void deleteEdge( EdgeKey key ) {
        LoomEdge edge = getEdge( key );
        gqs.deleteEdgeData( edge.getLabel().getReference().getEntitySetId(),
                edge.getLabel().getReference().getEntityId() );
        edges.delete( key );
    }

    @Override
    public void deleteEdges( UUID srcId ) {
        getEdges( new EdgeSelection(
                Optional.of( srcId ),
                Optional.absent(),
                Optional.absent(),
                Optional.absent(),
                Optional.absent() ) ).forEach( edge -> {
                    gqs.deleteEdgeData( edge.getLabel().getReference().getEntitySetId(),
                            edge.getLabel().getReference().getEntityId() );
                    edges.delete( edge.getKey() );
                } );
        ;
    }

}
