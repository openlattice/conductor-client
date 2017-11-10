package com.openlattice.postgres.mapstores;

import com.auth0.jwt.internal.org.apache.commons.lang3.RandomStringUtils;
import com.dataloom.hazelcast.HazelcastMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.openlattice.postgres.PostgresColumnDefinition;
import com.openlattice.postgres.ResultSetAdapters;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.openlattice.postgres.PostgresColumn.DESCRIPTION;
import static com.openlattice.postgres.PostgresColumn.ID;
import static com.openlattice.postgres.PostgresTable.ORGANIZATIONS;

public class OrganizationDescriptionsMapstore extends AbstractBasePostgresMapstore<UUID, String> {

    public OrganizationDescriptionsMapstore( HikariDataSource hds ) {
        super( HazelcastMap.ORGANIZATIONS_DESCRIPTIONS.name(), ORGANIZATIONS, hds );
    }

    @Override public List<PostgresColumnDefinition> keyColumns() {
        return ImmutableList.of( ID );
    }

    @Override public List<PostgresColumnDefinition> valueColumns() {
        return ImmutableList.of( DESCRIPTION );
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
        return ResultSetAdapters.description( rs );
    }

    @Override public UUID mapToKey( ResultSet rs ) {
        try {
            return ResultSetAdapters.id( rs );
        } catch ( SQLException ex ) {
            logger.error( "Unable to map ID to UUID class", ex );
            return null;
        }
    }

    @Override
    public Map<UUID, String> loadAll( Collection<UUID> keys ) {
        Map<UUID, String> result = Maps.newConcurrentMap();
        keys.parallelStream().forEach( id -> {
            String description = load(id);
            if ( description != null ) result.put( id, description );
        });
        return result;
    }

    @Override
    protected List<PostgresColumnDefinition> getInsertColumns() {
        return ImmutableList.of( ID, DESCRIPTION );
    }

    @Override public UUID generateTestKey() {
        return UUID.randomUUID();
    }

    @Override public String generateTestValue() {
        return RandomStringUtils.random( 10 );
    }
}
