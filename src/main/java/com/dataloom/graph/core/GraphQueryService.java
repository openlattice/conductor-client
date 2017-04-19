package com.dataloom.graph.core;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.data.EntityKey;
import com.dataloom.graph.EdgeSelection;
import com.dataloom.graph.core.objects.LoomEdgeKey;
import com.dataloom.graph.edge.EdgeKey;
import com.dataloom.graph.vertex.NeighborhoodSelection;
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

    private final PreparedStatement                          getVertexByEntityKeyQuery;
    private final PreparedStatement                          putVertexLookupIfAbsentQuery;
    private final PreparedStatement                          deleteVertexQuery;

    private final PreparedStatement                          getEdgeQuery;
    private final Map<Set<EdgeAttribute>, PreparedStatement> getEdgesQuery;
    private final PreparedStatement                          putEdgeQuery;
    private final PreparedStatement                          deleteEdgeQuery;
    private final PreparedStatement                          deleteEdgesBySrcIdQuery;
    private final PreparedStatement                          getEdgeCountForSrcQuery;
    private final PreparedStatement                          getEdgeCountForDstQuery;

    public GraphQueryService( Session session ) {
        this.session = session;

        this.getVertexByEntityKeyQuery = prepareGetVertexByEntityKeyQuery( session );
        this.putVertexLookupIfAbsentQuery = preparePutVertexLookupIfAbsentQuery( session );
        this.deleteVertexQuery = prepareDeleteVertexLookupQuery( session );

        this.getEdgeQuery = prepareGetEdgeQuery( session );
        this.getEdgesQuery = prepareGetEdgesQuery( session );
        this.putEdgeQuery = preparePutEdgeQuery( session );
        this.deleteEdgeQuery = prepareDeleteEdgeQuery( session );
        this.deleteEdgesBySrcIdQuery = prepareDeleteEdgesBySrcIdQuery( session );
        this.getEdgeCountForSrcQuery = prepareGetEdgeCountForSrcQuery( session );
        this.getEdgeCountForDstQuery = prepareGetEdgeCountForDstQuery( session );
    }

    private static PreparedStatement prepareGetVertexByEntityKeyQuery( Session session ) {
        return session
                .prepare( Table.VERTICES.getBuilder().buildLoadQuery() );
    }

    private static PreparedStatement preparePutVertexLookupIfAbsentQuery( Session session ) {
        return session
                .prepare( Table.VERTICES.getBuilder().buildStoreQuery().ifNotExists() );
    }

    private static PreparedStatement prepareDeleteVertexLookupQuery( Session session ) {
        return session
                .prepare( Table.VERTICES.getBuilder().buildDeleteQuery() );
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

    private static PreparedStatement prepareGetEdgeCountForSrcQuery( Session session ) {
        return session
                .prepare( QueryBuilder.select().all().from( Table.EDGES.getKeyspace(), Table.EDGES.getName() )
                        .where( QueryBuilder.eq( CommonColumns.SRC_ENTITY_KEY_ID.cql(),
                                CommonColumns.SRC_ENTITY_KEY_ID.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.EDGE_TYPE_ID.cql(),
                                CommonColumns.EDGE_TYPE_ID.bindMarker() ) )
                        .and( QueryBuilder.in( CommonColumns.DST_TYPE_ID.cql(),
                                CommonColumns.DST_TYPE_ID.bindMarker() ) ) );
    }

    private static PreparedStatement prepareGetEdgeCountForDstQuery( Session session ) {
        return session
                .prepare( QueryBuilder.select().countAll().from( Table.EDGES.getKeyspace(), Table.EDGES.getName() )
                        .where( QueryBuilder.eq( CommonColumns.DST_ENTITY_KEY_ID.cql(),
                                CommonColumns.DST_ENTITY_KEY_ID.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.EDGE_TYPE_ID.cql(),
                                CommonColumns.EDGE_TYPE_ID.bindMarker() ) )
                        .and( QueryBuilder.in( CommonColumns.SRC_VERTEX_TYPE_ID.cql(),
                                CommonColumns.SRC_VERTEX_TYPE_ID.bindMarker() ) ) );
    }

    public UUID getVertexByEntityKey( EntityKey entityKey ) {
        ResultSet rs = session.execute(
                getVertexByEntityKeyQuery.bind().set( CommonColumns.ENTITY_KEY.cql(), entityKey, EntityKey.class ) );
        Row row = rs.one();
        if ( row == null ) {
            return null;
        }
        return RowAdapters.id( row );
    }

    public ResultSetFuture putVertexIfAbsentAsync( UUID vertexId, EntityKey entityKey ) {
        return session.executeAsync( putVertexLookupIfAbsentQuery.bind()
                .set( CommonColumns.ENTITY_KEY.cql(), entityKey, EntityKey.class )
                .setUUID( CommonColumns.VERTEX_ID.cql(), vertexId ) );
    }

    public ResultSetFuture deleteVertexAsync( EntityKey entityKey ) {
        return session.executeAsync( deleteVertexQuery.bind()
                .set( CommonColumns.ENTITY_KEY.cql(), entityKey, EntityKey.class ) );
    }

    public LoomEdgeKey getEdge( EdgeKey key ) {
        BoundStatement stmt = getEdgeQuery.bind()
                .setUUID( CommonColumns.SRC_ENTITY_KEY_ID.cql(), key.getSrcEntityKeyId() )
                .setUUID( CommonColumns.DST_TYPE_ID.cql(), key.getDstTypeId() )
                .setUUID( CommonColumns.EDGE_TYPE_ID.cql(), key.getEdgeTypeId() )
                .setUUID( CommonColumns.DST_ENTITY_KEY_ID.cql(), key.getDstEntityKeyId() )
                .setUUID( CommonColumns.EDGE_ENTITY_KEY_ID.cql(), key.getEdgeEntityKeyId() );
        Row row = session.execute( stmt ).one();
        return row == null ? null : RowAdapters.loomEdge( row );
    }

    public Iterable<LoomEdgeKey> getEdges( EdgeSelection selection ) {
        Set<EdgeAttribute> attrs = EdgeAttribute.fromSelection( selection );
        BoundStatement stmt = getEdgesQuery.get( attrs ).bind();
        if ( selection.getOptionalSrcId().isPresent() )
            stmt.setUUID( CommonColumns.SRC_ENTITY_KEY_ID.cql(), selection.getOptionalSrcId().get() );
        if ( selection.getOptionalSrcType().isPresent() )
            stmt.setUUID( CommonColumns.SRC_VERTEX_TYPE_ID.cql(), selection.getOptionalSrcType().get() );
        if ( selection.getOptionalDstId().isPresent() )
            stmt.setUUID( CommonColumns.DST_ENTITY_KEY_ID.cql(), selection.getOptionalDstId().get() );
        if ( selection.getOptionalDstType().isPresent() )
            stmt.setUUID( CommonColumns.DST_TYPE_ID.cql(), selection.getOptionalDstType().get() );
        if ( selection.getOptionalEdgeType().isPresent() )
            stmt.setUUID( CommonColumns.EDGE_TYPE_ID.cql(), selection.getOptionalEdgeType().get() );
        ResultSet rs = session.execute( stmt );
        return Iterables.transform( rs, RowAdapters::loomEdge );
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
                .setUUID( CommonColumns.SRC_VERTEX_TYPE_ID.cql(), srcVertexEntityTypeId );
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

    public Stream<EdgeKey> getNeighborhood( NeighborhoodSelection ns ) {}

    public ResultSetFuture getNeighborEdgeCountAsync(
            UUID vertexId,
            UUID edgeTypeId,
            Set<UUID> neighborTypeIds,
            boolean vertexIsSrc ) {
        BoundStatement bs;
        if ( vertexIsSrc ) {
            bs = getEdgeCountForSrcQuery.bind()
                    .setUUID( CommonColumns.SRC_ENTITY_KEY_ID.cql(), vertexId )
                    .setUUID( CommonColumns.EDGE_TYPE_ID.cql(), edgeTypeId )
                    .setSet( CommonColumns.DST_TYPE_ID.cql(), neighborTypeIds, UUID.class );
        } else {
            bs = getEdgeCountForDstQuery.bind()
                    .setUUID( CommonColumns.DST_ENTITY_KEY_ID.cql(), vertexId )
                    .setUUID( CommonColumns.EDGE_TYPE_ID.cql(), edgeTypeId )
                    .setSet( CommonColumns.SRC_VERTEX_TYPE_ID.cql(), neighborTypeIds, UUID.class );
        }
        return session.executeAsync( bs );
    }

    /*
     * Used to help creating all the prepared statements needed for getEdges TODO Marked for refactoring (to HC), this
     * is too terrible
     */
    private enum EdgeAttribute {
        SRC_VERTEX_ID( CommonColumns.SRC_ENTITY_KEY_ID, es -> es.getOptionalSrcId().isPresent() ),
        SRC_VERTEX_TYPE_ID( CommonColumns.SRC_VERTEX_TYPE_ID, es -> es.getOptionalSrcType().isPresent() ),
        DST_VERTEX_ID( CommonColumns.DST_ENTITY_KEY_ID, es -> es.getOptionalDstId().isPresent() ),
        DST_VERTEX_TYPE_ID( CommonColumns.DST_TYPE_ID, es -> es.getOptionalDstType().isPresent() ),
        EDGE_TYPE_ID( CommonColumns.EDGE_TYPE_ID, es -> es.getOptionalEdgeType().isPresent() );

        private CommonColumns                    colRef;
        private Function<EdgeSelection, Boolean> hasAttribute;

        private EdgeAttribute( CommonColumns colRef, Function<EdgeSelection, Boolean> hasAttribute ) {
            this.colRef = colRef;
            this.hasAttribute = hasAttribute;
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

        public Clause getPreparedClause() {
            return QueryBuilder.eq( colRef.cql(), colRef.bindMarker() );
        }
    }
}
