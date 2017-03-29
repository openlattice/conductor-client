package com.dataloom.graph.core;

import java.util.UUID;

import com.dataloom.data.EntityKey;
import com.dataloom.graph.core.objects.EdgeKey;
import com.dataloom.graph.core.objects.GraphWrappedEdgeKey;
import com.dataloom.graph.core.objects.GraphWrappedEntityKey;
import com.dataloom.graph.core.objects.GraphWrappedVertexId;
import com.dataloom.graph.core.objects.LoomEdge;
import com.dataloom.graph.core.objects.LoomVertex;
import com.dataloom.hazelcast.HazelcastMap;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public class LoomGraph implements LoomGraphApi {
    
    private UUID graphId;
    
    private IMap<GraphWrappedVertexId, LoomVertex> vertices;
    
    private IMap<GraphWrappedEdgeKey, LoomEdge> edges;
    
    private GraphQueryService gqs;
    
    public LoomGraph( HazelcastInstance hazelcastInstance, GraphQueryService gqs ){
        this.vertices = hazelcastInstance.getMap( HazelcastMap.VERTICES.name() );
        this.edges = hazelcastInstance.getMap( HazelcastMap.EDGES.name() );
        
        this.gqs = gqs;
    }
    
    @Override
    public UUID getId() {
        return graphId;
    }
    
    @Override
    public LoomVertex addVertex( EntityKey entityKey ) {
        //Put if absent in verticesLookup
        //Generate random UUID, put if absent in vertices until it succeeds
        return gqs.addVertex( entityKey );
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
