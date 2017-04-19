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
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.cassandra.RowAdapters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private final PreparedStatement                                   deleteEdgesBySrcIdQuery;
    private final PreparedStatement                                   createVertexQuery;

    public GraphQueryService( String keyspace, Session session ) {
        this.session = session;
        this.createVertexQuery = prepareCreateVertexQuery( session );
        this.getEdgeQuery = prepareGetEdgeQuery( session );
        this.putEdgeQuery = preparePutEdgeQuery( session );
        this.deleteEdgeQuery = prepareDeleteEdgeQuery( session );
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

    public Stream<LoomEdge> getEdges( Map<CommonColumns, Set<UUID>> neighborhoodSelections ) {
        BoundStatement backedgeBs = backEdgeQueries.getUnchecked( neighborhoodSelections.keySet() ).bind();
        for ( Map.Entry<CommonColumns, Set<UUID>> e : neighborhoodSelections.entrySet() ) {
            backedgeBs.setSet( e.getKey().cql(), e.getValue(), UUID.class );
        }

        BoundStatement edgeBs = backEdgeQueries.getUnchecked( neighborhoodSelections.keySet() ).bind();
        for ( Map.Entry<CommonColumns, Set<UUID>> e : neighborhoodSelections.entrySet() ) {
            edgeBs.setSet( e.getKey().cql(), e.getValue(), UUID.class );
        }
        return Stream.concat(
                StreamUtil.stream( session.execute( backedgeBs ) ).map( RowAdapters::loomEdge ),
                StreamUtil.stream( session.execute( edgeBs ) ).map( RowAdapters::loomEdge ) );
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

    public ResultSetFuture createVertexAsync( UUID vertexId ) {
        return session.executeAsync( createVertexQuery.bind().setUUID( CommonColumns.VERTEX_ID.cql(), vertexId ) );
    }
}
