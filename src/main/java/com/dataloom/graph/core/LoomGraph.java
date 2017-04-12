package com.dataloom.graph.core;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.data.EntityKey;
import com.dataloom.graph.core.objects.EdgeKey;
import com.dataloom.graph.core.objects.EdgeSelection;
import com.dataloom.graph.core.objects.LoomEdge;
import com.dataloom.graph.core.objects.LoomVertex;
import com.dataloom.graph.core.objects.LoomVertexFuture;
import com.datastax.driver.core.ResultSetFuture;

public class LoomGraph implements LoomGraphApi {

    private static final Logger     logger = LoggerFactory.getLogger( LoomGraph.class );

    private GraphQueryService       gqs;

    public LoomGraph( GraphQueryService gqs ) {
        this.gqs = gqs;
    }

    @Override
    public LoomVertex getOrCreateVertex( EntityKey entityKey ) {
        return getOrCreateVertexAsync( entityKey ).get();
    }

    @Override
    public LoomVertexFuture getOrCreateVertexAsync( EntityKey entityKey ) {
        // TODO Auto-generated method stub
        return null;
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
    public void deleteVertex( UUID vertexId ) {
        // TODO delete all the incident edges
    }

    @Override
    public ResultSetFuture deleteVertexAsync( UUID vertexId ) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void addEdge( LoomVertex src, LoomVertex dst, EntityKey edgeLabel ) {
        addEdgeAsync( src, dst, edgeLabel ).getUninterruptibly();
    }

    @Override
    public void addEdge( EntityKey src, EntityKey dst, EntityKey edgeLabel ) {
        addEdgeAsync( src, dst, edgeLabel ).getUninterruptibly();
    }

    @Override
    public ResultSetFuture addEdgeAsync( LoomVertex src, LoomVertex dst, EntityKey edgeLabel ) {
        if ( src == null || dst == null ) {
            logger.error( "Edge for entity id {} cannot be created because one of its vertices was not created.",
                    edgeLabel.getEntityId() );
            return null;
        }
        
        return gqs.putEdgeAsync( src, dst, edgeLabel );
    }

    @Override
    public ResultSetFuture addEdgeAsync( EntityKey src, EntityKey dst, EntityKey edgeLabel ) {
        LoomVertex srcVertex = getVertexByEntityKey( src );
        LoomVertex dstVertex = getVertexByEntityKey( dst );
        
        return gqs.putEdgeAsync( srcVertex, dstVertex, edgeLabel );
    }
    
    @Override
    public LoomEdge getEdge( EdgeKey key ) {
        return gqs.getEdge( key );
    }

    @Override
    public Iterable<LoomEdge> getEdges( EdgeSelection selection ) {
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
