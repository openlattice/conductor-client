package com.dataloom.graph.core;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.graph.core.objects.EdgeCountEntryProcessor;
import com.dataloom.graph.edge.EdgeKey;
import com.dataloom.graph.edge.LoomEdge;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.kryptnostic.datastore.cassandra.CommonColumns;

public class LoomGraph implements LoomGraphApi {

    private static final Logger            logger = LoggerFactory.getLogger( LoomGraph.class );

    private final GraphQueryService        gqs;

    private final IMap<UUID, Neighborhood> edges;
    // vertex id -> dst type id -> edge type id -> dst entity key id
    private final IMap<UUID, Neighborhood> backedges;

    public LoomGraph( GraphQueryService gqs, HazelcastInstance hazelcastInstance ) {
        this.edges = hazelcastInstance.getMap( "" );
        this.backedges = hazelcastInstance.getMap( "" );

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
            UUID srcVertexEntitySetId,
            UUID dstVertexId,
            UUID dstVertexEntityTypeId,
            UUID dstVertexEntitySetId,
            UUID edgeEntityId,
            UUID edgeEntityTypeId,
            UUID edgeEntitySetId ) {
        addEdgeAsync( srcVertexId,
                srcVertexEntityTypeId,
                srcVertexEntitySetId,
                dstVertexId,
                dstVertexEntityTypeId,
                dstVertexEntitySetId,
                edgeEntityId,
                edgeEntityTypeId,
                edgeEntitySetId )
                        .forEach( ResultSetFuture::getUninterruptibly );
    }

    @Override
    public List<ResultSetFuture> addEdgeAsync(
            UUID srcVertexId,
            UUID srcVertexEntityTypeId,
            UUID srcVertexEntitySetId,
            UUID dstVertexId,
            UUID dstVertexEntityTypeId,
            UUID dstVertexEntitySetId,
            UUID edgeEntityId,
            UUID edgeEntityTypeId,
            UUID edgeEntitySetId ) {
        edges.evict( srcVertexId );
        backedges.evict( dstVertexId );
        return gqs.putEdgeAsync( srcVertexId,
                srcVertexEntityTypeId,
                srcVertexEntitySetId,
                dstVertexId,
                dstVertexEntityTypeId,
                dstVertexEntitySetId,
                edgeEntityId,
                edgeEntityTypeId,
                edgeEntitySetId );
    }

    @Override
    public void deleteVertex( UUID vertexId ) {
        deleteVertexAsync( vertexId ).forEach( ResultSetFuture::getUninterruptibly );
    }

    @Override
    public Stream<ResultSetFuture> deleteVertexAsync( UUID vertex ) {
        // TODO: Implement delete for neighborhoods
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
    public Stream<LoomEdge> getEdges( Map<CommonColumns, Set<UUID>> edgeSelection ) {
        return gqs.getFromEdgesTable( edgeSelection );
    }

    @Override
    public void deleteEdge( EdgeKey key ) {
        deleteEdgeAsync( key ).forEach( ResultSetFuture::getUninterruptibly );
    }

    @Override
    public List<ResultSetFuture> deleteEdgeAsync( EdgeKey edgeKey ) {
        edges.evict( edgeKey.getSrcEntityKeyId() );
        backedges.evict( edgeKey.getDstEntityKeyId() );
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

    @Override
    public ResultSetFuture getEdgeCount(
            UUID vertexId,
            UUID associationTypeId,
            Set<UUID> neighborTypeIds,
            boolean vertexIsSrc ) {

        return gqs.getNeighborEdgeCountAsync( vertexId, associationTypeId, neighborTypeIds, vertexIsSrc );
    }

    @Override
    public int getHazelcastEdgeCount(
            UUID vertexId,
            UUID associationTypeId,
            Set<UUID> neighborTypeIds,
            boolean vertexIsSrc ) {
        if ( vertexIsSrc ) {
            return (Integer) edges.executeOnKey( vertexId,
                    new EdgeCountEntryProcessor( associationTypeId, neighborTypeIds ) );
        }

        return (Integer) backedges
                .executeOnKey( vertexId, new EdgeCountEntryProcessor( associationTypeId, neighborTypeIds ) );
    }

}
