package com.dataloom.graph.core;

import com.dataloom.graph.edge.EdgeKey;
import com.dataloom.graph.edge.LoomEdge;
import com.dataloom.streams.StreamUtil;
import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.cassandra.RowAdapters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

public class GraphQueryService {
    private static final Logger logger = LoggerFactory
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

    public GraphQueryService( String keyspace, Session session ) {
        this.session = session;
        this.createVertexQuery = prepareCreateVertexQuery( session );
        this.getEdgeQuery = prepareGetEdgeQuery( session );
        this.putEdgeQuery = preparePutEdgeQuery( session );
        this.putBackEdgeQuery = preparePutBackEdgeQuery( session );
        this.deleteEdgeQuery = prepareDeleteEdgeQuery( session );
        this.deleteBackEdgeQuery = prepareDeleteBackEdgeQuery( session );
        this.deleteEdgesBySrcIdQuery = prepareDeleteEdgesBySrcIdQuery( session );
        this.edgeQueries = CacheBuilder
                .newBuilder()
                .maximumSize( 32 )
                .build( new CacheLoader<Set<CommonColumns>, PreparedStatement>() {
                    @Override
                    public PreparedStatement load( Set<CommonColumns> key ) throws Exception {
                        Select.Where q = QueryBuilder.select().all().from( keyspace, Table.EDGES.getName() ).where();
                        for ( CommonColumns c : key ) {
                            q = q.and( QueryBuilder.in( c.cql(), c.bindMarker() ) );
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
                        Select.Where q = QueryBuilder.select().all().from( keyspace, Table.EDGES.getName() ).where();
                        for ( CommonColumns c : key ) {
                            q = q.and( QueryBuilder.in( c.cql(), c.bindMarker() ) );
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
        for ( Map.Entry<CommonColumns, Set<UUID>> e : neighborhoodSelections.entrySet() ) {
            edgeBs.setSet( e.getKey().cql(), e.getValue(), UUID.class );
        }

        BoundStatement backedgeBs = backEdgeQueries.getUnchecked( neighborhoodSelections.keySet() ).bind();
        for ( Map.Entry<CommonColumns, Set<UUID>> e : neighborhoodSelections.entrySet() ) {
            backedgeBs.setSet( e.getKey().cql(), e.getValue(), UUID.class );
        }

        return Stream.concat(
                StreamUtil.stream( session.execute( edgeBs ) ).map( RowAdapters::loomEdge ),
                StreamUtil.stream( session.execute( backedgeBs ) ).map( RowAdapters::loomEdge )
        );
    }

    public List<ResultSetFuture> putEdgeAsync(
            UUID srcVertexId,
            UUID srcVertexEntityTypeId,
            UUID dstVertexId,
            UUID dstVertexEntityTypeId,
            UUID edgeEntityId,
            UUID edgeEntityTypeId ) {

        BoundStatement edgeBs = bindEdge( putEdgeQuery.bind(),
                srcVertexId,
                srcVertexEntityTypeId,
                dstVertexId,
                dstVertexEntityTypeId,
                edgeEntityId,
                edgeEntityTypeId );

        BoundStatement backedgeBs = bindEdge( putBackEdgeQuery.bind(),
                dstVertexId,
                dstVertexEntityTypeId,
                srcVertexId,
                srcVertexEntityTypeId,
                edgeEntityId,
                edgeEntityTypeId );
        return ImmutableList.of( session.executeAsync( edgeBs ), session.executeAsync( backedgeBs ) );
    }

    private BoundStatement bindEdge(
            BoundStatement bs,
            UUID srcVertexId,
            UUID srcVertexEntityTypeId,
            UUID dstVertexId,
            UUID dstVertexEntityTypeId,
            UUID edgeEntityId,
            UUID edgeEntityTypeId ) {
        return bs
                .setUUID( CommonColumns.SRC_ENTITY_KEY_ID.cql(), srcVertexId )
                .setUUID( CommonColumns.DST_TYPE_ID.cql(), dstVertexEntityTypeId )
                .setUUID( CommonColumns.EDGE_TYPE_ID.cql(), edgeEntityTypeId )
                .setUUID( CommonColumns.DST_ENTITY_KEY_ID.cql(), dstVertexId )
                .setUUID( CommonColumns.EDGE_ENTITY_KEY_ID.cql(), edgeEntityId )
                .setUUID( CommonColumns.SRC_TYPE_ID.cql(), srcVertexEntityTypeId );
    }

    public void deleteEdge( LoomEdge key ) {
        deleteEdgeAsync( key ).forEach( ResultSetFuture::getUninterruptibly );
    }

    public List<ResultSetFuture> deleteEdgeAsync( LoomEdge edge ) {
        EdgeKey key = edge.getKey();
        BoundStatement edgeBs = bindDeleteEdge( deleteEdgeQuery.bind(),
                key.getSrcEntityKeyId(),
                key.getDstEntityKeyId(),
                key.getDstTypeId(),
                key.getEdgeEntityKeyId(),
                key.getEdgeTypeId() );

        BoundStatement backedgeBs = bindDeleteEdge( deleteEdgeQuery.bind(),
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

    public ResultSetFuture createVertexAsync( UUID vertexId ) {
        return session.executeAsync( createVertexQuery.bind().setUUID( CommonColumns.VERTEX_ID.cql(), vertexId ) );
    }
}
