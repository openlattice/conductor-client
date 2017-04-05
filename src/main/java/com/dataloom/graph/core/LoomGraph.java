package com.dataloom.graph.core;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.data.EntityKey;
import com.dataloom.graph.core.objects.EdgeKey;
import com.dataloom.graph.core.objects.EdgeSelection;
import com.dataloom.graph.core.objects.LoomEdge;
import com.dataloom.graph.core.objects.LoomVertex;
import com.dataloom.hazelcast.HazelcastMap;
import com.google.common.base.Optional;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public class LoomGraph implements LoomGraphApi {

    private static final Logger     logger = LoggerFactory.getLogger( LoomGraph.class );

    private IMap<UUID, LoomVertex>  vertices;
    private IMap<EntityKey, UUID>   verticesLookup;

    private IMap<EdgeKey, LoomEdge> edges;
    private GraphQueryService       gqs;

    public LoomGraph( HazelcastInstance hazelcastInstance, GraphQueryService gqs ) {
        this.vertices = hazelcastInstance.getMap( HazelcastMap.VERTICES.name() );
        this.verticesLookup = hazelcastInstance.getMap( HazelcastMap.VERTICES_LOOKUP.name() );
        this.edges = hazelcastInstance.getMap( HazelcastMap.EDGES.name() );

        this.gqs = gqs;
    }

    @Override
    public LoomVertex getOrCreateVertex( EntityKey entityKey ) {
        // Put if absent in verticesLookup
        // Generate random UUID, put if absent in vertices until it succeeds
        while ( true ) {
            UUID proposedId = UUID.randomUUID();
            UUID currentId = verticesLookup.putIfAbsent( entityKey, proposedId );
            if ( currentId != null ) {
                return vertices.get( currentId );
            } else {
                LoomVertex vertex = new LoomVertex( proposedId, entityKey );
                if ( vertices.putIfAbsent( proposedId, vertex ) == null ) {
                    return vertex;
                } else {
                    // creation failed. Rollback the insertion to verticesLookup, and restart the UUID generation
                    // process again.
                    verticesLookup.remove( entityKey );
                }
            }
        }
    }

    @Override
    public LoomVertex getVertex( UUID vertexId ) {
        return vertices.get( vertexId );
    }

    @Override
    public void deleteVertex( UUID vertexId ) {
        // TODO delete all the incident edges
    }

    @Override
    public LoomEdge addEdge( LoomVertex src, LoomVertex dst, EntityKey reference ) {
        EdgeKey key = new EdgeKey( src.getKey(), dst.getKey(), reference );
        LoomEdge edge = new LoomEdge(
                key,
                src.getReference().getEntitySetId(),
                dst.getReference().getEntitySetId() );
        if ( edges.putIfAbsent( key, edge ) == null ) {
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
        return edges.get( key );
    }

    @Override
    public Iterable<LoomEdge> getEdges( EdgeSelection selection ) {
        return gqs.getEdges( selection );
    }

    @Override
    public void deleteEdge( EdgeKey key ) {
        gqs.deleteEdgeData( key.getReference() );
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
                    gqs.deleteEdgeData( edge.getReference() );
                    edges.delete( edge.getKey() );
                } );
    }

}
