package com.dataloom.graph.core;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.data.EntityKey;
import com.dataloom.graph.core.objects.EdgeKey;
import com.dataloom.graph.core.objects.EdgeSelection;
import com.dataloom.graph.core.objects.LoomEdge;
import com.dataloom.graph.core.objects.LoomVertex;
import com.dataloom.graph.core.objects.LoomVertexFuture;
import com.dataloom.streams.StreamUtil;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;

public class LoomGraph implements LoomGraphApi {

    private static final Logger logger = LoggerFactory.getLogger( LoomGraph.class );

    private GraphQueryService   gqs;

    public LoomGraph( GraphQueryService gqs ) {
        this.gqs = gqs;
    }

    @Override
    public LoomVertex getOrCreateVertex( EntityKey entityKey ) {
        return getOrCreateVertexAsync( entityKey ).get();
    }

    @Override
    public LoomVertexFuture getOrCreateVertexAsync( EntityKey entityKey ) {
        return new LoomVertexFuture( entityKey );
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
        deleteVertexAsync( vertexId ).forEach( ResultSetFuture::getUninterruptibly );
    }

    @Override
    public List<ResultSetFuture> deleteVertexAsync( UUID vertexId ) {
        LoomVertex vertex = getVertexById( vertexId );
        List<ResultSetFuture> futures = new ArrayList<>();
        if( vertex != null ){
            futures.add( gqs.deleteVertexLookupAsync( vertex.getReference() ) );
            futures.add( gqs.deleteVertexAsync( vertexId ) );
        EdgeSelection fixSrc = new EdgeSelection(
                Optional.of( vertexId ),
                Optional.absent(),
                Optional.absent(),
                Optional.absent(),
                Optional.absent() );
        EdgeSelection fixDst = new EdgeSelection(
                Optional.absent(),
                Optional.absent(),
                Optional.of( vertexId ),
                Optional.absent(),
                Optional.absent() );
        Iterable<LoomEdge> edges = Iterables.concat( getEdges( fixSrc ), getEdges( fixDst ) );

        StreamUtil.stream( edges ).map( edge -> deleteEdgeAsync( edge.getKey() ) )
                .collect( Collectors.toCollection( () -> futures ) );
        }
        return futures;
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
