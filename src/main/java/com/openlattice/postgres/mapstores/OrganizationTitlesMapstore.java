package com.openlattice.postgres.mapstores;

import com.auth0.jwt.internal.org.apache.commons.lang3.RandomStringUtils;
import com.dataloom.hazelcast.HazelcastMap;
import com.google.common.collect.ImmutableList;
import com.openlattice.postgres.PostgresColumnDefinition;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

import static com.openlattice.postgres.PostgresColumn.ID;
import static com.openlattice.postgres.PostgresColumn.NULLABLE_TITLE;
import static com.openlattice.postgres.PostgresTable.ORGANIZATIONS;

public class OrganizationTitlesMapstore extends AbstractBasePostgresMapstore<UUID, String> {

    public OrganizationTitlesMapstore( HikariDataSource hds ) {
        super( HazelcastMap.ORGANIZATIONS_TITLES.name(), ORGANIZATIONS, hds );
    }

    @Override public List<PostgresColumnDefinition> keyColumns() {
        return ImmutableList.of( ID );
    }

    @Override public List<PostgresColumnDefinition> valueColumns() {
        return ImmutableList.of( NULLABLE_TITLE );
    }

    @Override public void bind( PreparedStatement ps, UUID key, String value ) throws SQLException {
        ps.setObject( 1, key );
        ps.setString( 2, value );

        // UPDATE
        ps.setString( 3, value );
    }

    @Override public void bind( PreparedStatement ps, UUID key ) throws SQLException {
        ps.setObject( 1, key );
    }

    @Override public String mapToValue( ResultSet rs ) throws SQLException {
        return rs.getString( NULLABLE_TITLE.getName() );
    }

    @Override public UUID mapToKey( ResultSet rs ) {
        try {
            return rs.getObject( ID.getName(), UUID.class );
        } catch ( SQLException ex ) {
            logger.error( "Unable to map ID to UUID class", ex );
            return null;
        }
    }

    @Override
    protected List<PostgresColumnDefinition> getInsertColumns() {
        return ImmutableList.of( ID, NULLABLE_TITLE );
    }

    @Override public UUID generateTestKey() {
        return UUID.randomUUID();
    }

    @Override public String generateTestValue() {
        return RandomStringUtils.random( 10 );
    }
}
