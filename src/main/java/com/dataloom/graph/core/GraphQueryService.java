package com.dataloom.graph.core;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.data.EntityKey;
import com.dataloom.graph.core.objects.EdgeKey;
import com.dataloom.graph.core.objects.EdgeSelection;
import com.dataloom.graph.core.objects.LoomEdge;
import com.dataloom.graph.core.objects.LoomVertex;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.collect.Sets;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.cassandra.RowAdapters;

import jersey.repackaged.com.google.common.collect.Iterables;

public class GraphQueryService {
    private static final Logger                              logger = LoggerFactory
            .getLogger( GraphQueryService.class );
    private final Session                                    session;

    private final PreparedStatement                          getVertexByIdQuery;
    private final PreparedStatement                          getVertexByEntityKeyQuery;
    private final PreparedStatement                          putVertexLookupIfAbsentQuery;
    private final PreparedStatement                          updateVertexLookupIfExistsQuery;
    private final PreparedStatement                          putVertexIfAbsentQuery;
    private final PreparedStatement                          deleteVertexQuery;
    private final PreparedStatement                          deleteVertexLookupQuery;

    private final PreparedStatement                          getEdgeQuery;
    private final Map<Set<EdgeAttribute>, PreparedStatement> getEdgesQuery;
    private final PreparedStatement                          putEdgeQuery;
    private final PreparedStatement                          deleteEdgeQuery;
    private final PreparedStatement                          deleteEdgesBySrcIdQuery;

    public GraphQueryService( Session session ) {
        this.session = session;

        this.getVertexByIdQuery = prepareGetVertexByIdQuery( session );
        this.getVertexByEntityKeyQuery = prepareGetVertexByEntityKeyQuery( session );
        this.putVertexLookupIfAbsentQuery = preparePutVertexLookupIfAbsentQuery( session );
        this.updateVertexLookupIfExistsQuery = prepareUpdateVertexLookupIfExistsQuery( session );
        this.putVertexIfAbsentQuery = preparePutVertexIfAbsentQuery( session );
        this.deleteVertexQuery = prepareDeleteVertexQuery( session );
        this.deleteVertexLookupQuery = prepareDeleteVertexLookupQuery( session );

        this.getEdgeQuery = prepareGetEdgeQuery( session );
        this.getEdgesQuery = prepareGetEdgesQuery( session );
        this.putEdgeQuery = preparePutEdgeQuery( session );
        this.deleteEdgeQuery = prepareDeleteEdgeQuery( session );
        this.deleteEdgesBySrcIdQuery = prepareDeleteEdgesBySrcIdQuery( session );
    }

    public LoomVertex getVertexById( UUID vertexId ) {
        ResultSet rs = session.execute( getVertexByIdQuery.bind().setUUID( CommonColumns.VERTEX_ID.cql(), vertexId ) );
        Row row = rs.one();
        if ( row == null ) {
            return null;
        }
        return RowAdapters.loomVertex( row );
    }

    public LoomVertex getVertexByEntityKey( EntityKey entityKey ) {
        ResultSet rs = session.execute(
                getVertexByEntityKeyQuery.bind().set( CommonColumns.ENTITY_KEY.cql(), entityKey, EntityKey.class ) );
        Row row = rs.one();
        if ( row == null ) {
            return null;
        }
        return RowAdapters.loomVertex( row );
    }

    public ResultSetFuture putVertexIfAbsentAsync( UUID vertexId, EntityKey entityKey ) {
        return session.executeAsync( putVertexIfAbsentQuery.bind()
                .setUUID( CommonColumns.VERTEX_ID.cql(), vertexId )
                .set( CommonColumns.ENTITY_KEY.cql(), entityKey, EntityKey.class ) );
    }

    public ResultSetFuture putVertexLookUpIfAbsentAsync( UUID vertexId, EntityKey entityKey ) {
        return session.executeAsync( putVertexLookupIfAbsentQuery.bind()
                .set( CommonColumns.ENTITY_KEY.cql(), entityKey, EntityKey.class )
                .setUUID( CommonColumns.VERTEX_ID.cql(), vertexId ) );
    }

    public ResultSetFuture updateVertexLookupIfExistsAsync( UUID vertexId, EntityKey entityKey ) {
        return session.executeAsync( updateVertexLookupIfExistsQuery.bind()
                .setUUID( CommonColumns.VERTEX_ID.cql(), vertexId )
                .set( CommonColumns.ENTITY_KEY.cql(), entityKey, EntityKey.class ) );
    }

    public ResultSetFuture deleteVertexAsync( UUID vertexId ){
        return session.executeAsync( deleteVertexQuery.bind()
                .setUUID( CommonColumns.VERTEX_ID.cql(), vertexId ) );
    }
    
    public ResultSetFuture deleteVertexLookupAsync( EntityKey entityKey ){
        return session.executeAsync( deleteVertexLookupQuery.bind()
                .set( CommonColumns.ENTITY_KEY.cql(), entityKey, EntityKey.class ) );
    }
    
    public LoomEdge getEdge( EdgeKey key ) {
        BoundStatement stmt = getEdgeQuery.bind()
                .setUUID( CommonColumns.SRC_VERTEX_ID.cql(), key.getSrcId() )
                .setUUID( CommonColumns.DST_VERTEX_ID.cql(), key.getDstId() )
                .setUUID( CommonColumns.EDGE_TYPE_ID.cql(), key.getReference().getEntitySetId() )
                .setString( CommonColumns.EDGE_ENTITYID.cql(), key.getReference().getEntityId() )
                .setUUID( CommonColumns.SYNCID.cql(), key.getReference().getSyncId() );
        Row row = session.execute( stmt ).one();
        return row == null ? null : RowAdapters.loomEdge( row );
    }

    public Iterable<LoomEdge> getEdges( EdgeSelection selection ) {
        Set<EdgeAttribute> attrs = EdgeAttribute.fromSelection( selection );
        BoundStatement stmt = getEdgesQuery.get( attrs ).bind();
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

    public ResultSetFuture putEdgeAsync( LoomVertex src, LoomVertex dst, EntityKey edgeLabel ) {
        BoundStatement stmt = putEdgeQuery.bind()
                .setUUID( CommonColumns.SRC_VERTEX_ID.cql(), src.getKey() )
                .setUUID( CommonColumns.DST_VERTEX_ID.cql(), dst.getKey() )
                .setUUID( CommonColumns.EDGE_TYPE_ID.cql(), edgeLabel.getEntitySetId() )
                .setString( CommonColumns.EDGE_ENTITYID.cql(), edgeLabel.getEntityId() )
                .setUUID( CommonColumns.SYNCID.cql(), edgeLabel.getSyncId() )
                .setUUID( CommonColumns.SRC_VERTEX_TYPE_ID.cql(), src.getReference().getEntitySetId() )
                .setUUID( CommonColumns.DST_VERTEX_TYPE_ID.cql(), dst.getReference().getEntitySetId() );
        return session.executeAsync( stmt );
    }

    public void deleteEdge( EdgeKey key ) {
        deleteEdgeAsync( key ).getUninterruptibly();
    }

    public ResultSetFuture deleteEdgeAsync( EdgeKey key ) {
        BoundStatement stmt = deleteEdgeQuery.bind()
                .setUUID( CommonColumns.SRC_VERTEX_ID.cql(), key.getSrcId() )
                .setUUID( CommonColumns.DST_VERTEX_ID.cql(), key.getDstId() )
                .setUUID( CommonColumns.EDGE_TYPE_ID.cql(), key.getReference().getEntitySetId() )
                .setString( CommonColumns.EDGE_ENTITYID.cql(), key.getReference().getEntityId() )
                .setUUID( CommonColumns.SYNCID.cql(), key.getReference().getSyncId() );
        return session.executeAsync( stmt );
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

    private static PreparedStatement preparePutVertexIfAbsentQuery( Session session ) {
        return session
                .prepare( Table.VERTICES.getBuilder().buildStoreQuery().ifNotExists() );
    }

    private static PreparedStatement preparePutVertexLookupIfAbsentQuery( Session session ) {
        return session
                .prepare( Table.VERTICES_LOOKUP.getBuilder().buildStoreQuery().ifNotExists() );
    }

    private static PreparedStatement prepareUpdateVertexLookupIfExistsQuery( Session session ) {
        return session
                .prepare( QueryBuilder.update( Table.VERTICES_LOOKUP.getKeyspace(), Table.VERTICES_LOOKUP.getName() )
                        .with( QueryBuilder.set( CommonColumns.VERTEX_ID.cql(), CommonColumns.VERTEX_ID.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.ENTITY_KEY.cql(),
                                CommonColumns.ENTITY_KEY.bindMarker() ) )
                        .ifExists() );
    }
    
    private static PreparedStatement prepareDeleteVertexQuery( Session session ) {
        return session
                .prepare( Table.VERTICES.getBuilder().buildDeleteQuery() );
    }

    private static PreparedStatement prepareDeleteVertexLookupQuery( Session session ) {
        return session
                .prepare( Table.VERTICES_LOOKUP.getBuilder().buildDeleteQuery() );
    }

    private static PreparedStatement prepareGetEdgeQuery( Session session ) {
        return session
                .prepare( Table.EDGES.getBuilder().buildLoadQuery() );
    }

    private static Map<Set<EdgeAttribute>, PreparedStatement> prepareGetEdgesQuery( Session session ) {
        Map<Set<EdgeAttribute>, PreparedStatement> map = new HashMap<>();
        for ( Set<EdgeAttribute> subset : Sets.powerSet( EnumSet.allOf( EdgeAttribute.class ) ) ) {
            map.put( subset, prepareGetEdgesQuery( session, subset ) );
        }
        return map;
    }

    private static PreparedStatement prepareGetEdgesQuery( Session session, Set<EdgeAttribute> subset ) {
        Select.Where stmt = QueryBuilder.select().all().from( Table.EDGES.getKeyspace(), Table.EDGES.getName() )
                .allowFiltering().where();
        for ( EdgeAttribute attr : subset ) {
            stmt.and( attr.getPreparedClause() );
        }
        return session.prepare( stmt );

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

    /*
     * Used to help creating all the prepared statements needed for getEdges
     * TODO Marked for refactoring (to HC), this is too terrible
     */
    private enum EdgeAttribute {
        SRC_VERTEX_ID( CommonColumns.SRC_VERTEX_ID, es -> es.getOptionalSrcId().isPresent() ),
        SRC_VERTEX_TYPE_ID( CommonColumns.SRC_VERTEX_TYPE_ID, es -> es.getOptionalSrcType().isPresent() ),
        DST_VERTEX_ID( CommonColumns.DST_VERTEX_ID, es -> es.getOptionalDstId().isPresent() ),
        DST_VERTEX_TYPE_ID( CommonColumns.DST_VERTEX_TYPE_ID, es -> es.getOptionalDstType().isPresent() ),
        EDGE_TYPE_ID( CommonColumns.EDGE_TYPE_ID, es -> es.getOptionalEdgeType().isPresent() );

        private CommonColumns                    colRef;
        private Function<EdgeSelection, Boolean> hasAttribute;

        private EdgeAttribute( CommonColumns colRef, Function<EdgeSelection, Boolean> hasAttribute ) {
            this.colRef = colRef;
            this.hasAttribute = hasAttribute;
        }

        public Clause getPreparedClause() {
            return QueryBuilder.eq( colRef.cql(), colRef.bindMarker() );
        }

        public static Set<EdgeAttribute> fromSelection( EdgeSelection es ) {
            Set<EdgeAttribute> ans = EnumSet.noneOf( EdgeAttribute.class );
            for ( EdgeAttribute attr : values() ) {
                if ( attr.hasAttribute.apply( es ) ) {
                    ans.add( attr );
                }
            }
            return ans;
        }
    }
}
