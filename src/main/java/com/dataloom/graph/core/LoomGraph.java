package com.dataloom.graph.core;

import java.util.UUID;

import com.dataloom.data.EntityKey;
import com.dataloom.graph.core.objects.EdgeKey;
import com.dataloom.graph.core.objects.GraphWrappedEdgeKey;
import com.dataloom.graph.core.objects.GraphWrappedEntityKey;
import com.dataloom.graph.core.objects.GraphWrappedVertexId;
import com.dataloom.graph.core.objects.LoomEdge;
import com.dataloom.graph.core.objects.LoomVertex;
import com.dataloom.graph.core.objects.VertexLabel;
import com.dataloom.hazelcast.HazelcastMap;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public class LoomGraph implements LoomGraphApi {
    
    private UUID graphId;
    
    private static IMap<GraphWrappedVertexId, LoomVertex> vertices;
    private static IMap<GraphWrappedEntityKey, UUID> verticesLookup;
    
    private static IMap<GraphWrappedEdgeKey, LoomEdge> edges;
    private static GraphQueryService gqs;
    
    public static void init( HazelcastInstance hazelcastInstance, GraphQueryService gqs ){
        LoomGraph.vertices = hazelcastInstance.getMap( HazelcastMap.VERTICES.name() );
        LoomGraph.edges = hazelcastInstance.getMap( HazelcastMap.EDGES.name() );
        
        LoomGraph.gqs = gqs;
    }
    
    public LoomGraph(){
        
    }
    
    @Override
    public UUID getId() {
        return graphId;
    }
    
    @Override
    public LoomVertex getOrCreateVertex( EntityKey entityKey ) {
        //Put if absent in verticesLookup
        //Generate random UUID, put if absent in vertices until it succeeds
        GraphWrappedEntityKey lookupKey = new GraphWrappedEntityKey( graphId, entityKey );

        while( true ){
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
                    // creation failed. Rollback the insertion to verticesLookup, and restart the UUID generation process again.
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
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public LoomEdge getEdge( EdgeKey key ) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Iterable<LoomEdge> getEdges( EdgeSelection selection ) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void deleteEdge( EdgeKey key ) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void deleteEdges( UUID srcId ) {
        // TODO Auto-generated method stub
        
    }

}
