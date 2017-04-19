package com.dataloom.graph.core;

import com.dataloom.graph.edge.EdgeKey;
import com.dataloom.graph.edge.LoomEdge;
import com.dataloom.graph.vertex.NeighborhoodSelection;
import com.datastax.driver.core.*;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.cassandra.RowAdapters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

public class GraphQueryService {
    private static final Logger logger = LoggerFactory
            .getLogger( GraphQueryService.class );
    private final Session                  session;
    private final Cache<Set<CommonColumns>, PreparedStatement> edgeQueries;
    private final PreparedStatement getEdgeQuery;
    private final PreparedStatement putEdgeQuery;
    private final PreparedStatement deleteEdgeQuery;
    private final PreparedStatement deleteEdgesBySrcIdQuery;
    private final PreparedStatement createVertexQuery;

    public GraphQueryService( Session session ) {
        this.session = session;
        this.createVertexQuery = prepareCreateVertexQuery( session );
        this.getEdgeQuery = prepareGetEdgeQuery( session );
        this.putEdgeQuery = preparePutEdgeQuery( session );
        this.deleteEdgeQuery = prepareDeleteEdgeQuery( session );
        this.deleteEdgesBySrcIdQuery = prepareDeleteEdgesBySrcIdQuery( session );
        this.edgeQueries = CacheBuilder<Set<CommonColumns>,PreparedStatement>
    }

    private static PreparedStatement prepareCreateVertexQuery( Session session ) {
        return session.prepare( Table.VERTICES.getBuilder().buildStoreQuery() );
    }

    private static PreparedStatement prepareGetEdgeQuery( Session session ) {
        return session
                .prepare( Table.EDGES.getBuilder().buildLoadQuery() );
    }

    private static PreparedStatement preparePutEdgeQuery( Session session ) {
        return session
                .prepare( Table.EDGES.getBuilder().buildStoreQuery() );
    }

    private static PreparedStatement prepareDeleteEdgeQuery( Session session ) {
        return session
                .prepare( Table.EDGES.getBuilder().buildDeleteQuery() );
    }

    private static PreparedStatement prepareDeleteEdgesBySrcIdQuery( Session session ) {
        return session
                .prepare( Table.EDGES.getBuilder().buildDeleteByPartitionKeyQuery() );
    }

    public LoomEdge getEdge( EdgeKey key ) {
        BoundStatement stmt = getEdgeQuery.bind()
                .setUUID( CommonColumns.SRC_ENTITY_KEY_ID.cql(), key.getSrcEntityKeyId() )
                .setUUID( CommonColumns.DST_TYPE_ID.cql(), key.getDstTypeId() )
                .setUUID( CommonColumns.EDGE_TYPE_ID.cql(), key.getEdgeTypeId() )
                .setUUID( CommonColumns.DST_ENTITY_KEY_ID.cql(), key.getDstEntityKeyId() )
                .setUUID( CommonColumns.EDGE_ENTITY_KEY_ID.cql(), key.getEdgeEntityKeyId() );
        Row row = session.execute( stmt ).one();
        return row == null ? null : RowAdapters.loomEdge( row );
    }

    public Iterable<LoomEdge> getEdges( NeighborhoodSelection selection ) {
        //        Set<EdgeAttribute> attrs = EdgeAttribute.fromSelection( selection );
        //        BoundStatement stmt = getEdgesQuery.get( attrs ).bind();
        //        if ( selection.getOptionalSrcId().isPresent() )
        //            stmt.setUUID( CommonColumns.SRC_ENTITY_KEY_ID.cql(), selection.getOptionalSrcId().get() );
        //        if ( selection.getOptionalSrcType().isPresent() )
        //            stmt.setUUID( CommonColumns.SRC_TYPE_ID.cql(), selection.getOptionalSrcType().get() );
        //        if ( selection.getOptionalDstId().isPresent() )
        //            stmt.setUUID( CommonColumns.DST_ENTITY_KEY_ID.cql(), selection.getOptionalDstId().get() );
        //        if ( selection.getOptionalDstType().isPresent() )
        //            stmt.setUUID( CommonColumns.DST_TYPE_ID.cql(), selection.getOptionalDstType().get() );
        //        if ( selection.getOptionalEdgeType().isPresent() )
        //            stmt.setUUID( CommonColumns.EDGE_TYPE_ID.cql(), selection.getOptionalEdgeType().get() );
        //        ResultSet rs = session.execute( stmt );
        //        return Iterables.transform( rs, RowAdapters::loomEdge );
        return null;

    }

    public ResultSetFuture putEdgeAsync(
            UUID srcVertexId,
            UUID srcVertexEntityTypeId,
            UUID dstVertexId,
            UUID dstVertexEntityTypeId,
            UUID edgeEntityId,
            UUID edgeEntityTypeId ) {
        BoundStatement stmt = putEdgeQuery.bind()
                .setUUID( CommonColumns.SRC_ENTITY_KEY_ID.cql(), srcVertexId )
                .setUUID( CommonColumns.DST_TYPE_ID.cql(), dstVertexEntityTypeId )
                .setUUID( CommonColumns.EDGE_TYPE_ID.cql(), edgeEntityTypeId )
                .setUUID( CommonColumns.DST_ENTITY_KEY_ID.cql(), dstVertexId )
                .setUUID( CommonColumns.EDGE_ENTITY_KEY_ID.cql(), edgeEntityId )
                .setUUID( CommonColumns.SRC_TYPE_ID.cql(), srcVertexEntityTypeId );
        return session.executeAsync( stmt );
    }

    public void deleteEdge( EdgeKey key ) {
        deleteEdgeAsync( key ).getUninterruptibly();
    }

    public ResultSetFuture deleteEdgeAsync( EdgeKey key ) {
        BoundStatement stmt = deleteEdgeQuery.bind()
                .setUUID( CommonColumns.SRC_ENTITY_KEY_ID.cql(), key.getSrcEntityKeyId() )
                .setUUID( CommonColumns.DST_ENTITY_KEY_ID.cql(), key.getDstEntityKeyId() )
                .setUUID( CommonColumns.DST_TYPE_ID.cql(), key.getDstTypeId() )
                .setUUID( CommonColumns.EDGE_TYPE_ID.cql(), key.getEdgeTypeId() )
                .setUUID( CommonColumns.EDGE_ENTITY_KEY_ID.cql(), key.getEdgeEntityKeyId() );
        return session.executeAsync( stmt );
    }

    public void deleteEdgesBySrcId( UUID srcId ) {
        session.execute(
                deleteEdgesBySrcIdQuery.bind().setUUID( CommonColumns.SRC_ENTITY_KEY_ID.cql(), srcId ) );
    }

    public Stream<EdgeKey> getNeighborhood( NeighborhoodSelection ns ) {
        return Stream.of();
    }

    public ResultSetFuture createVertexAsync( UUID vertexId ) {
        return session.executeAsync( createVertexQuery.bind().setUUID( CommonColumns.VERTEX_ID.cql(), vertexId ) );
    }
}
