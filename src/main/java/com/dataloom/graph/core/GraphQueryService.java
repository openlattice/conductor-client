package com.dataloom.graph.core;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.data.EntityKey;
import com.dataloom.graph.core.objects.EdgeSelection;
import com.dataloom.graph.core.objects.LoomEdge;
import com.dataloom.graph.core.objects.LoomVertex;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.cassandra.RowAdapters;

import jersey.repackaged.com.google.common.collect.Iterables;

public class GraphQueryService {
    private static final Logger     logger = LoggerFactory.getLogger( GraphQueryService.class );
    private final Session           session;

    private final PreparedStatement getVertexQuery;
    private final PreparedStatement createVertexLookupIfNotExistsQuery;
    private final PreparedStatement createVertexIfNotExistsQuery;
    private final PreparedStatement updateVertexIdQuery;

    private final PreparedStatement getEdgesQuery;
    private final PreparedStatement deleteEdgesBySrcIdQuery;

    public GraphQueryService( Session session ) {
        this.session = session;

        this.getVertexQuery = prepareGetVertexQuery( session );
        this.createVertexLookupIfNotExistsQuery = prepareCreateVertexLookupIfNotExistsQuery( session );
        this.createVertexIfNotExistsQuery = prepareCreateVertexIfNotExistsQuery( session );
        this.updateVertexIdQuery = prepareUpdateVertexIdQuery( session );

        this.getEdgesQuery = prepareGetEdgesQuery( session );
        this.deleteEdgesBySrcIdQuery = prepareDeleteEdgesBySrcIdQuery( session );
    }

    public LoomVertex getVertex( EntityKey key ) {
        ResultSet rs = session
                .execute( getVertexQuery.bind().set( CommonColumns.ENTITY_KEY.cql(), key, EntityKey.class ) );
        return RowAdapters.loomVertex( rs.one() );
    }

    public LoomVertex createVertexIfNotExists( EntityKey key ) {
        UUID vertexId = UUID.randomUUID();
        ResultSet rs = session.execute(
                createVertexLookupIfNotExistsQuery.bind().set( CommonColumns.ENTITY_KEY.cql(), key, EntityKey.class )
                        .setUUID( CommonColumns.VERTEX_ID.cql(), vertexId ) );
        if ( rs.wasApplied() ) return RowAdapters.loomVertex( rs.one() );

        boolean created = false;
        while ( !created ) {
            ResultSet vertexRs = session
                    .execute( createVertexIfNotExistsQuery.bind().setUUID( CommonColumns.VERTEX_ID.cql(), vertexId )
                            .set( CommonColumns.ENTITY_KEY.cql(), key, EntityKey.class ) );
            if ( vertexRs.wasApplied() ) {
                created = true;
            } else {
                vertexId = UUID.randomUUID();
                session.execute( updateVertexIdQuery.bind().set( CommonColumns.ENTITY_KEY.cql(), key, EntityKey.class )
                        .setUUID( CommonColumns.VERTEX_ID.cql(), vertexId ) );
            }
        }
        return new LoomVertex( vertexId, key );
    }

    public Iterable<LoomEdge> getEdges( EdgeSelection selection ) {
        BoundStatement stmt = getEdgesQuery.bind();
        if ( selection.getOptionalSrcId().isPresent() )
            stmt.setUUID( CommonColumns.SRC_VERTEX_ID.cql(), selection.getOptionalSrcId().get() );
        if ( selection.getOptionalSrcType().isPresent() )
            stmt.setUUID( CommonColumns.SRC_VERTEX_TYPE_ID.cql(), selection.getOptionalSrcType().get() );
        if ( selection.getOptionalDstId().isPresent() )
            stmt.setUUID( CommonColumns.DST_VERTEX_ID.cql(), selection.getOptionalDstId().get() );
        if ( selection.getOptionalDstType().isPresent() )
            stmt.setUUID( CommonColumns.DST_VERTEX_TYPE_ID.cql(), selection.getOptionalDstType().get() );
        if ( selection.getOptionalEdgeType().isPresent() )
            stmt.setUUID( CommonColumns.EDGE_TYPE_ID.cql(), selection.getOptionalEdgeType().get() );
        ResultSet rs = session.execute( stmt );
        return Iterables.transform( rs, RowAdapters::loomEdge );
    }

    public void deleteEdgesBySrcId( UUID srcId ) {
        session.execute(
                deleteEdgesBySrcIdQuery.bind().setUUID( CommonColumns.SRC_VERTEX_ID.cql(), srcId ) );
    }

    private static PreparedStatement prepareGetVertexQuery( Session session ) {
        return session.prepare( QueryBuilder.select().all()
                .from( Table.VERTICES_LOOKUP.getKeyspace(), Table.VERTICES_LOOKUP.getName() )
                .where( QueryBuilder.eq( CommonColumns.ENTITY_KEY.cql(), CommonColumns.ENTITY_KEY.bindMarker() ) ) );
    }

    private static PreparedStatement prepareCreateVertexLookupIfNotExistsQuery( Session session ) {
        return session.prepare(
                QueryBuilder.insertInto( Table.VERTICES_LOOKUP.getKeyspace(), Table.VERTICES_LOOKUP.getName() )
                        .ifNotExists().value( CommonColumns.ENTITY_KEY.cql(), CommonColumns.ENTITY_KEY.bindMarker() )
                        .value( CommonColumns.VERTEX_ID.cql(), CommonColumns.VERTEX_ID.bindMarker() ) );
    }

    private static PreparedStatement prepareCreateVertexIfNotExistsQuery( Session session ) {
        return session.prepare( QueryBuilder.insertInto( Table.VERTICES.getKeyspace(), Table.VERTICES.getName() )
                .ifNotExists().value( CommonColumns.VERTEX_ID.cql(), CommonColumns.VERTEX_ID.bindMarker() )
                .value( CommonColumns.ENTITY_KEY.cql(), CommonColumns.ENTITY_KEY.bindMarker() ) );
    }

    private static PreparedStatement prepareUpdateVertexIdQuery( Session session ) {
        return session.prepare( QueryBuilder
                .update( Table.VERTICES_LOOKUP.getKeyspace(), Table.VERTICES_LOOKUP.getName() )
                .with( QueryBuilder.set( CommonColumns.VERTEX_ID.cql(), CommonColumns.VERTEX_ID.bindMarker() ) )
                .where( QueryBuilder.eq( CommonColumns.ENTITY_KEY.cql(), CommonColumns.ENTITY_KEY.bindMarker() ) ) );
    }

    private static PreparedStatement prepareGetEdgesQuery( Session session ) {
        return session
                .prepare( QueryBuilder.select().all().from( Table.EDGES.getKeyspace(), Table.EDGES.getName() )
                        .allowFiltering()
                        .where( QueryBuilder.eq( CommonColumns.SRC_VERTEX_ID.cql(),
                                CommonColumns.SRC_VERTEX_ID.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.SRC_VERTEX_TYPE_ID.cql(),
                                CommonColumns.SRC_VERTEX_TYPE_ID.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.DST_VERTEX_ID.cql(),
                                CommonColumns.DST_VERTEX_ID.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.DST_VERTEX_TYPE_ID.cql(),
                                CommonColumns.DST_VERTEX_TYPE_ID.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.EDGE_TYPE_ID.cql(),
                                CommonColumns.EDGE_TYPE_ID.bindMarker() ) ) );

    }

    private static PreparedStatement prepareDeleteEdgesBySrcIdQuery( Session session ) {
        return session
                .prepare( Table.EDGES.getBuilder().buildDeleteByPartitionKeyQuery() );
    }
}
