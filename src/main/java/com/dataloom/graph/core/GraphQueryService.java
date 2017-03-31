package com.dataloom.graph.core;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.graph.core.objects.EdgeSelection;
import com.dataloom.graph.core.objects.LoomEdge;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.cassandra.RowAdapters;

import jersey.repackaged.com.google.common.collect.Iterables;

public class GraphQueryService {
    private static final Logger     logger = LoggerFactory.getLogger( GraphQueryService.class );
    private final Session           session;
    private final ObjectMapper      mapper;

    private final PreparedStatement deleteAllEdgesForSrcQuery;
    private final PreparedStatement getEdgesQuery;

    public GraphQueryService( Session session, ObjectMapper mapper ) {
        this.session = session;
        this.mapper = mapper;

        this.deleteAllEdgesForSrcQuery = prepareDeleteAllEdgesForSrcQuery( session );
        this.getEdgesQuery = prepareGetEdgesQuery( session );

    }

    public Iterable<LoomEdge> getEdges( EdgeSelection selection ) {
        BoundStatement stmt = getEdgesQuery.bind();
        if ( selection.getOptionalSrcId().isPresent() ) stmt.bind( CommonColumns.SRC_VERTEX_ID.cql(), selection.getOptionalSrcId().get() );
        if ( selection.getOptionalSrcType().isPresent() ) stmt.bind( CommonColumns.SRC_VERTEX_TYPE_ID.cql(), selection.getOptionalSrcType().get() );
        if ( selection.getOptionalDstId().isPresent() ) stmt.bind( CommonColumns.DST_VERTEX_ID.cql(), selection.getOptionalDstId().get() );
        if ( selection.getOptionalDstType().isPresent() ) stmt.bind( CommonColumns.DST_VERTEX_TYPE_ID.cql(), selection.getOptionalDstType().get() );
        if ( selection.getOptionalEdgeType().isPresent() ) stmt.bind( CommonColumns.EDGE_TYPE_ID.cql(), selection.getOptionalEdgeType().get() );
        ResultSet rs = session.execute( stmt );
        return Iterables.transform( rs, RowAdapters::loomEdge );
    }

    public void deleteEdges( UUID srcId ) {
        session.execute( deleteAllEdgesForSrcQuery.bind().setUUID( CommonColumns.SRC_VERTEX_ID.cql(), srcId ) );
    }

    private static PreparedStatement prepareDeleteAllEdgesForSrcQuery( Session session ) {
        return session.prepare(
                QueryBuilder.delete().from( Table.EDGES.getKeyspace(), Table.EDGES.getName() ).where( QueryBuilder
                        .eq( CommonColumns.SRC_VERTEX_ID.cql(), CommonColumns.SRC_VERTEX_ID.bindMarker() ) ) );
    }

    private static PreparedStatement prepareGetEdgesQuery( Session session ) {
        return session
                .prepare( QueryBuilder.select().all().from( Table.EDGES.getKeyspace(), Table.EDGES.getName() ).where(
                        QueryBuilder.eq( CommonColumns.SRC_VERTEX_ID.cql(),
                                CommonColumns.SRC_VERTEX_ID.bindMarker() ) )
                        .and(
                                QueryBuilder.eq( CommonColumns.SRC_VERTEX_TYPE_ID.cql(),
                                        CommonColumns.SRC_VERTEX_TYPE_ID.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.DST_VERTEX_ID.cql(),
                                CommonColumns.DST_VERTEX_ID.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.DST_VERTEX_TYPE_ID.cql(),
                                CommonColumns.DST_VERTEX_TYPE_ID.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.EDGE_TYPE_ID.cql(),
                                CommonColumns.EDGE_TYPE_ID.bindMarker() ) ) );

    }

}
