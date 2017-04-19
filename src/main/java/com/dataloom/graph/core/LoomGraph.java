package com.dataloom.graph.core;

import com.dataloom.graph.edge.EdgeKey;
import com.dataloom.graph.edge.LoomEdge;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.hazelcast.core.HazelcastInstance;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

public class LoomGraph implements LoomGraphApi {

    private static final Logger logger = LoggerFactory.getLogger( LoomGraph.class );

    private final GraphQueryService gqs;

    public LoomGraph( GraphQueryService gqs, HazelcastInstance hazelcastInstance ) {
        this.gqs = gqs;
    }

    @Override
    public void createVertex( UUID vertexId ) {
        createVertexAsync( vertexId ).getUninterruptibly();
    }

    @Override
    public ResultSetFuture createVertexAsync( UUID vertexId ) {
        return gqs.createVertexAsync( vertexId );
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
                edgeEntityTypeId )
                .forEach( ResultSetFuture::getUninterruptibly );
    }

    @Override
    public List<ResultSetFuture> addEdgeAsync(
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
    public Stream<ResultSetFuture> deleteVertexAsync( UUID vertex ) {
        return gqs
                .getEdges( ImmutableMap.of( CommonColumns.SRC_ENTITY_KEY_ID, ImmutableSet.of( vertex ) ) )
                .map( LoomEdge::getKey )
                .map( this::deleteEdgeAsync )
                .flatMap( List::stream );
    }

    @Override
    public LoomEdge getEdge( EdgeKey key ) {
        return gqs.getEdge( key );
    }

    @Override
    public void deleteEdge( EdgeKey key ) {
        gqs.deleteEdge( getEdge( key ) );
    }

    @Override
    public List<ResultSetFuture> deleteEdgeAsync( EdgeKey edgeKey ) {
        return gqs.deleteEdgeAsync( getEdge( edgeKey ) );
    }

    @Override
    public void deleteEdges( UUID srcId ) {
        gqs.deleteEdgesBySrcId( srcId );
    }

    @Override
    public Stream<LoomEdge> getEdgesAndNeighborsForVertex( UUID vertexId ) {
        return gqs
                .getEdges( ImmutableMap.of( CommonColumns.SRC_ENTITY_KEY_ID, ImmutableSet.of( vertexId ) ) );
    }

}
