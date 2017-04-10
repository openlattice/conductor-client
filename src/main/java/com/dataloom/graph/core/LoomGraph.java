package com.dataloom.graph.core;

import java.util.Map;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.data.EntityKey;
import com.dataloom.graph.core.objects.EdgeKey;
import com.dataloom.graph.core.objects.EdgeSelection;
import com.dataloom.graph.core.objects.LoomEdge;
import com.dataloom.graph.core.objects.LoomVertex;
import com.dataloom.hazelcast.HazelcastMap;
import com.google.common.collect.SetMultimap;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.kryptnostic.datastore.services.CassandraDataManager;

public class LoomGraph implements LoomGraphApi {

    private static final Logger     logger = LoggerFactory.getLogger( LoomGraph.class );

    private GraphQueryService       gqs;

    public LoomGraph( GraphQueryService gqs ) {
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
    public LoomVertex getVertexById( UUID vertexId ) {
        return gqs.getVertexById( vertexId );
    }

    @Override
    public LoomVertex getVertexByEntityKey( EntityKey reference ) {
        return gqs.getVertexByEntityKey( reference );
    }

    @Override
    public LoomVertexFuture createVertexAsync( EntityKey entityKey ){
        return LoomVertexFuture( entityKey );
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
        LoomVertex srcVertex = getVertex( src );
        LoomVertex dstVertex = getVertex( dst );
        if( srcVertex == null || dstVertex == null ){
            logger.error( "Edge for entity id {} cannot be created because one of its vertices was not created.",
                    label.getEntityId() );
        }
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
        cdm.deleteEntity( key.getReference() );
        edges.delete( key );
    }

    @Override
    public void deleteEdges( UUID srcId ) {
        gqs.deleteEdgesBySrcId( srcId );
    }

    @Override
    public LoomVertexFuture deleteVertexAsync( UUID vertexId ) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public LoomEdgeFuture addEdgeAsync( LoomVertex src, LoomVertex dst, EntityKey edgeLabel ) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public LoomEdgeFuture deleteEdgeAsync( EdgeKey edgeKey ) {
        // TODO Auto-generated method stub
        return null;
    }
    
}
