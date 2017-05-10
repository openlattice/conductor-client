package com.dataloom.graph.core;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.graph.edge.EdgeKey;
import com.dataloom.graph.edge.LoomEdge;
import com.dataloom.streams.StreamUtil;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.cassandra.RowAdapters;

public class GraphQueryService {
    private static final Logger                                       logger = LoggerFactory
            .getLogger( GraphQueryService.class );

    private final Session                                             session;
    private final LoadingCache<Set<CommonColumns>, PreparedStatement> edgeQueries;
    private final LoadingCache<Set<CommonColumns>, PreparedStatement> backEdgeQueries;
    private final PreparedStatement                                   getEdgeQuery;
    private final PreparedStatement                                   putEdgeQuery;
    private final PreparedStatement                                   deleteEdgeQuery;
    private final PreparedStatement                                   deleteBackEdgeQuery;
    private final PreparedStatement                                   deleteEdgesBySrcIdQuery;
    private final PreparedStatement                                   createVertexQuery;
    private final PreparedStatement                                   putBackEdgeQuery;
    private final PreparedStatement                                   getEdgeCountForSrcQuery;
    private final PreparedStatement                                   getEdgeCountForDstQuery;

    public GraphQueryService( Session session ) {
        this.session = session;
        this.createVertexQuery = prepareCreateVertexQuery( session );
        this.getEdgeQuery = prepareGetEdgeQuery( session );
        this.putEdgeQuery = preparePutEdgeQuery( session );
        this.putBackEdgeQuery = preparePutBackEdgeQuery( session );
        this.deleteEdgeQuery = prepareDeleteEdgeQuery( session );
        this.deleteBackEdgeQuery = prepareDeleteBackEdgeQuery( session );
        this.deleteEdgesBySrcIdQuery = prepareDeleteEdgesBySrcIdQuery( session );
        this.getEdgeCountForSrcQuery = prepareGetEdgeCountForSrcQuery( session );
        this.getEdgeCountForDstQuery = prepareGetEdgeCountForDstQuery( session );

        this.edgeQueries = CacheBuilder
                .newBuilder()
                .maximumSize( 32 )
                .build( new CacheLoader<Set<CommonColumns>, PreparedStatement>() {
                    @Override
                    public PreparedStatement load( Set<CommonColumns> key ) throws Exception {
                        Select.Where q = QueryBuilder.select().all()
                                .from( Table.EDGES.getKeyspace(), Table.EDGES.getName() ).allowFiltering().where();
                        for ( CommonColumns c : key ) {
                            q = q.and( c.eq() );
                            // q = q.and( QueryBuilder.in( c.cql(), c.bindMarker() ) );
                        }
                        return session.prepare( q );
                    }
                } );
        this.backEdgeQueries = CacheBuilder
                .newBuilder()
                .maximumSize( 32 )
                .build( new CacheLoader<Set<CommonColumns>, PreparedStatement>() {
                    @Override
                    public PreparedStatement load( Set<CommonColumns> key ) throws Exception {
                        Select.Where q = QueryBuilder.select().all()
                                .from( Table.BACK_EDGES.getKeyspace(), Table.BACK_EDGES.getName() ).allowFiltering()
                                .where();
                        for ( CommonColumns c : key ) {
                            q = q.and( c.eq() );
                            // q = q.and( QueryBuilder.in( c.cql(), c.bindMarker() ) );
                        }
                        return session.prepare( q );
                    }
                } );
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

    private static PreparedStatement preparePutBackEdgeQuery( Session session ) {
        return session
                .prepare( Table.BACK_EDGES.getBuilder().buildStoreQuery() );
    }

    private static PreparedStatement prepareDeleteEdgeQuery( Session session ) {
        return session
                .prepare( Table.EDGES.getBuilder().buildDeleteQuery() );
    }

    private static PreparedStatement prepareGetEdgeCountForSrcQuery( Session session ) {
        return session
                .prepare( restrictEdgeSearch( QueryBuilder.select().countAll().from( Table.EDGES.getKeyspace(),
                        Table.EDGES.getName() ) ) );
    }

    private static PreparedStatement prepareGetEdgeCountForDstQuery( Session session ) {
        return session
                .prepare( restrictEdgeSearch( QueryBuilder.select().countAll().from( Table.BACK_EDGES.getKeyspace(),
                        Table.BACK_EDGES.getName() ) ) );
    }

    private static Select.Where restrictEdgeSearch( Select query ) {
        return query
                .where( QueryBuilder.eq( CommonColumns.SRC_ENTITY_KEY_ID.cql(),
                        CommonColumns.SRC_ENTITY_KEY_ID.bindMarker() ) )
                .and( QueryBuilder.eq( CommonColumns.EDGE_TYPE_ID.cql(),
                        CommonColumns.EDGE_TYPE_ID.bindMarker() ) )
                .and( QueryBuilder.in( CommonColumns.DST_TYPE_ID.cql(),
                        CommonColumns.DST_TYPE_ID.bindMarker() ) );
    }

    private static PreparedStatement prepareDeleteBackEdgeQuery( Session session ) {
        return session
                .prepare( Table.BACK_EDGES.getBuilder().buildDeleteQuery() );
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

    public Stream<LoomEdge> getEdges( Map<CommonColumns, Set<UUID>> neighborhoodSelections ) {

        BoundStatement edgeBs = edgeQueries.getUnchecked( neighborhoodSelections.keySet() ).bind();

        BoundStatement backedgeBs = backEdgeQueries.getUnchecked( neighborhoodSelections.keySet() ).bind();

        return Stream.concat(
                treeBind( neighborhoodSelections.entrySet().iterator(), edgeBs )
                        .map( ResultSetFuture::getUninterruptibly )
                        .flatMap( StreamUtil::stream )
                        .map( RowAdapters::loomEdge ),
                treeBind( neighborhoodSelections.entrySet().iterator(), backedgeBs )
                        .map( ResultSetFuture::getUninterruptibly )
                        .flatMap( StreamUtil::stream )
                        .map( RowAdapters::loomBackEdge ) )
                .distinct();
    }

    private Stream<ResultSetFuture> treeBind( Iterator<Map.Entry<CommonColumns, Set<UUID>>> i, BoundStatement bs ) {
        if ( i.hasNext() ) {
            final Map.Entry<CommonColumns, Set<UUID>> e = i.next();
            final Set<UUID> ids = e.getValue();
            return ids.parallelStream().flatMap( id -> {
                bs.setUUID( e.getKey().cql(), id );
                return treeBind( i, bs );
            } );
        } else {
            return Stream.of( session.executeAsync( bs ) );
        }
    }

    public List<ResultSetFuture> putEdgeAsync(
            UUID srcVertexId,
            UUID srcVertexEntityTypeId,
            UUID srcVertexEntitySetId,
            UUID dstVertexId,
            UUID dstVertexEntityTypeId,
            UUID dstVertexEntitySetId,
            UUID edgeEntityId,
            UUID edgeEntityTypeId,
            UUID edgeEntitySetId ) {

        BoundStatement edgeBs = bindEdge( putEdgeQuery.bind(),
                srcVertexId,
                srcVertexEntityTypeId,
                srcVertexEntitySetId,
                dstVertexId,
                dstVertexEntityTypeId,
                dstVertexEntitySetId,
                edgeEntityId,
                edgeEntityTypeId,
                edgeEntitySetId );

        BoundStatement backedgeBs = bindEdge( putBackEdgeQuery.bind(),
                dstVertexId,
                dstVertexEntityTypeId,
                dstVertexEntitySetId,
                srcVertexId,
                srcVertexEntityTypeId,
                srcVertexEntitySetId,
                edgeEntityId,
                edgeEntityTypeId,
                edgeEntitySetId );
        return ImmutableList.of( session.executeAsync( edgeBs ), session.executeAsync( backedgeBs ) );
    }

    private BoundStatement bindEdge(
            BoundStatement bs,
            UUID srcVertexId,
            UUID srcVertexEntityTypeId,
            UUID srcVertexEntitySetId,
            UUID dstVertexId,
            UUID dstVertexEntityTypeId,
            UUID dstVertexEntitySetId,
            UUID edgeEntityId,
            UUID edgeEntityTypeId,
            UUID edgeEntitySetId ) {
        return bs
                .setUUID( CommonColumns.SRC_ENTITY_KEY_ID.cql(), srcVertexId )
                .setUUID( CommonColumns.DST_TYPE_ID.cql(), dstVertexEntityTypeId )
                .setUUID( CommonColumns.EDGE_TYPE_ID.cql(), edgeEntityTypeId )
                .setUUID( CommonColumns.DST_ENTITY_KEY_ID.cql(), dstVertexId )
                .setUUID( CommonColumns.EDGE_ENTITY_KEY_ID.cql(), edgeEntityId )
                .setUUID( CommonColumns.SRC_TYPE_ID.cql(), srcVertexEntityTypeId )
                .setUUID( CommonColumns.SRC_ENTITY_SET_ID.cql(), srcVertexEntitySetId )
                .setUUID( CommonColumns.DST_ENTITY_SET_ID.cql(), dstVertexEntitySetId )
                .setUUID( CommonColumns.EDGE_ENTITY_SET_ID.cql(), edgeEntitySetId );
    }

    public void deleteEdge( LoomEdge key ) {
        deleteEdgeAsync( key ).forEach( ResultSetFuture::getUninterruptibly );
    }

    public List<ResultSetFuture> deleteEdgeAsync( LoomEdge edge ) {
        if( edge == null ){
            return ImmutableList.of();
        }
        EdgeKey key = edge.getKey();
        BoundStatement edgeBs = bindDeleteEdge( deleteEdgeQuery.bind(),
                key.getSrcEntityKeyId(),
                key.getDstEntityKeyId(),
                key.getDstTypeId(),
                key.getEdgeEntityKeyId(),
                key.getEdgeTypeId() );

        BoundStatement backedgeBs = bindDeleteEdge( deleteBackEdgeQuery.bind(),
                key.getDstEntityKeyId(),
                key.getSrcEntityKeyId(),
                edge.getSrcType(),
                key.getEdgeEntityKeyId(),
                key.getEdgeTypeId() );

        return ImmutableList.of( session.executeAsync( edgeBs ), session.executeAsync( backedgeBs ) );
    }

    private BoundStatement bindDeleteEdge(
            BoundStatement bs,
            UUID srcVertexId,
            UUID dstVertexId,
            UUID dstVertexEntityTypeId,
            UUID edgeEntityId,
            UUID edgeEntityTypeId ) {
        return bs
                .setUUID( CommonColumns.SRC_ENTITY_KEY_ID.cql(), srcVertexId )
                .setUUID( CommonColumns.DST_TYPE_ID.cql(), dstVertexEntityTypeId )
                .setUUID( CommonColumns.EDGE_TYPE_ID.cql(), edgeEntityTypeId )
                .setUUID( CommonColumns.DST_ENTITY_KEY_ID.cql(), dstVertexId )
                .setUUID( CommonColumns.EDGE_ENTITY_KEY_ID.cql(), edgeEntityId );
    }

    public void deleteEdgesBySrcId( UUID srcId ) {
        session.execute(
                deleteEdgesBySrcIdQuery.bind().setUUID( CommonColumns.SRC_ENTITY_KEY_ID.cql(), srcId ) );
    }

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
                    .setSet( CommonColumns.DST_TYPE_ID.cql(), neighborTypeIds );
        } else {
            bs = getEdgeCountForDstQuery.bind()
                    .setUUID( CommonColumns.SRC_ENTITY_KEY_ID.cql(), vertexId )
                    .setUUID( CommonColumns.EDGE_TYPE_ID.cql(), edgeTypeId )
                    .setSet( CommonColumns.DST_TYPE_ID.cql(), neighborTypeIds );
        }
        return session.executeAsync( bs );
    }

    public ResultSetFuture createVertexAsync( UUID vertexId ) {
        return session.executeAsync( createVertexQuery.bind().setUUID( CommonColumns.VERTEX_ID.cql(), vertexId ) );
    }
}
