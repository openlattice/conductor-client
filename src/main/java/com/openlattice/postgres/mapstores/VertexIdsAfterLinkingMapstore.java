package com.openlattice.postgres.mapstores;

import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.linking.LinkingVertexKey;
import com.google.common.collect.ImmutableList;
import com.openlattice.postgres.PostgresColumnDefinition;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

import static com.openlattice.postgres.PostgresColumn.*;
import static com.openlattice.postgres.PostgresTable.VERTEX_IDS_AFTER_LINKING;

public class VertexIdsAfterLinkingMapstore extends AbstractBasePostgresMapstore<LinkingVertexKey, UUID> {

    public VertexIdsAfterLinkingMapstore( HikariDataSource hds ) {
        super( HazelcastMap.VERTEX_IDS_AFTER_LINKING.name(), VERTEX_IDS_AFTER_LINKING, hds );
    }

    @Override protected List<PostgresColumnDefinition> keyColumns() {
        return ImmutableList.of( GRAPH_ID, VERTEX_ID );
    }

    @Override protected List<PostgresColumnDefinition> valueColumns() {
        return ImmutableList.of( NEW_VERTEX_ID );
    }

    @Override protected void bind( PreparedStatement ps, LinkingVertexKey key, UUID value ) throws SQLException {
        bind( ps, key );
        ps.setObject( 3, value );

        // UPDATE
        ps.setObject( 4, value );
    }

    @Override protected void bind( PreparedStatement ps, LinkingVertexKey key ) throws SQLException {
        ps.setObject( 1, key.getGraphId() );
        ps.setObject( 2, key.getVertexId() );
    }

    @Override protected UUID mapToValue( ResultSet rs ) throws SQLException {
        return rs.getObject( NEW_VERTEX_ID.getName(), UUID.class );
    }

    @Override protected LinkingVertexKey mapToKey( ResultSet rs ) {
        try {
            UUID graphId = rs.getObject( GRAPH_ID.getName(), UUID.class );
            UUID vertexId = rs.getObject( VERTEX_ID.getName(), UUID.class );
            return new LinkingVertexKey( graphId, vertexId );
        } catch ( SQLException e ) {
            logger.debug( "Unable to map LinkingVertexKey", e );
            return null;
        }
    }

    @Override public LinkingVertexKey generateTestKey() {
        return new LinkingVertexKey( UUID.randomUUID(), UUID.randomUUID() );
    }

    @Override public UUID generateTestValue() {
        return UUID.randomUUID();
    }
}
