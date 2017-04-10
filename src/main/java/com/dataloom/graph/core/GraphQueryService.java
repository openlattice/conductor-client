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
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.cassandra.RowAdapters;

import jersey.repackaged.com.google.common.collect.Iterables;

public class GraphQueryService {
    private static final Logger     logger = LoggerFactory.getLogger( GraphQueryService.class );
    private final Session           session;

    private final PreparedStatement getVertexByIdQuery;
    private final PreparedStatement getVertexByEntityKeyQuery;
    private final PreparedStatement putVertexLookupIfAbsentQuery;
    private final PreparedStatement putVertexIfAbsentQuery;
    
    private final PreparedStatement getEdgesQuery;
    private final PreparedStatement deleteEdgesBySrcIdQuery;

    public GraphQueryService( Session session ) {
        this.session = session;

        this.getVertexByIdQuery = prepareGetVertexByIdQuery( session );
        this.getVertexByEntityKeyQuery = prepareGetVertexByEntityKeyQuery( session );
        this.putVertexLookupIfAbsentQuery = preparePutVertexLookupIfAbsentQuery( session );
        this.putVertexIfAbsentQuery = preparePutVertexIfAbsentQuery( session );

        this.getEdgesQuery = prepareGetEdgesQuery( session );
        this.deleteEdgesBySrcIdQuery = prepareDeleteEdgesBySrcIdQuery( session );
    }

    public LoomVertex getVertexById( UUID vertexId ){
        ResultSet rs = session.execute( getVertexByIdQuery.bind().setUUID( CommonColumns.VERTEX_ID.cql(), vertexId ) );
        Row row = rs.one();
        if( row == null ){
            return null;
        }
        return RowAdapters.loomVertex( row );
    }
    
    public LoomVertex getVertexByEntityKey( EntityKey entityKey ){
        ResultSet rs = session.execute( getVertexByEntityKeyQuery.bind().set( CommonColumns.ENTITY_KEY.cql(), entityKey, EntityKey.class ) );
        Row row = rs.one();
        if( row == null ){
            return null;
        }
        return RowAdapters.loomVertex( row );
    }
    
    public ResultSetFuture putVertexIfAbsent( UUID vertexId, EntityKey entityKey ){
        return session.executeAsync( putVertexIfAbsentQuery.bind()
                .set( CommonColumns.ENTITY_KEY.cql(), entityKey, EntityKey.class )
                .setUUID( CommonColumns.VERTEX_ID.cql(), vertexId ) );
    }
    
    public ResultSetFuture putVertexLookUpIfAbsent( UUID vertexId, EntityKey entityKey ){
        return session.executeAsync( putVertexLookupIfAbsentQuery.bind()
                .set( CommonColumns.ENTITY_KEY.cql(), entityKey, EntityKey.class )
                .setUUID( CommonColumns.VERTEX_ID.cql(), vertexId ) );
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

    private static PreparedStatement prepareGetVertexByIdQuery( Session session ) {
        return session
                .prepare( Table.VERTICES.getBuilder().buildLoadQuery() );
    }

    private static PreparedStatement prepareGetVertexByEntityKeyQuery( Session session ) {
        return session
                .prepare( Table.VERTICES_LOOKUP.getBuilder().buildLoadQuery() );
    }

    private static PreparedStatement preparePutVertexIfAbsentQuery( Session session ){
        return session
                .prepare( Table.VERTICES.getBuilder().buildStoreQuery().ifNotExists() );        
    }
    
    private static PreparedStatement preparePutVertexLookupIfAbsentQuery( Session session ){
        return session
                .prepare( Table.VERTICES_LOOKUP.getBuilder().buildStoreQuery().ifNotExists() );        
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
