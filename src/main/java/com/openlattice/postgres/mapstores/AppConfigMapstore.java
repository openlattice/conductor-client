package com.openlattice.postgres.mapstores;

import com.dataloom.apps.AppConfigKey;
import com.dataloom.hazelcast.HazelcastMap;
import com.google.common.collect.ImmutableList;
import com.openlattice.postgres.PostgresColumnDefinition;
import com.openlattice.postgres.PostgresTable;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

import static com.openlattice.postgres.PostgresColumn.*;

public class AppConfigMapstore extends AbstractBasePostgresMapstore<AppConfigKey, UUID> {
    public AppConfigMapstore( HikariDataSource hds ) {
        super( HazelcastMap.APP_CONFIGS.name(), PostgresTable.APP_CONFIGS, hds );
    }

    @Override protected List<PostgresColumnDefinition> keyColumns() {
        return ImmutableList.of( APP_ID, ORGANIZATION_ID, CONFIG_TYPE_ID );
    }

    @Override protected List<PostgresColumnDefinition> valueColumns() {
        return ImmutableList.of( ENTITY_SET_ID );
    }

    @Override protected void bind( PreparedStatement ps, AppConfigKey key, UUID value ) throws SQLException {
        bind( ps, key );

        ps.setObject( 4, value );

        // UPDATE
        ps.setObject( 5, value );
    }

    @Override protected void bind( PreparedStatement ps, AppConfigKey key ) throws SQLException {
        ps.setObject( 1, key.getAppId() );
        ps.setObject( 2, key.getOrganizationId() );
        ps.setObject( 3, key.getAppTypeId() );
    }

    @Override protected UUID mapToValue( ResultSet rs ) throws SQLException {
        return rs.getObject( ENTITY_SET_ID.getName(), UUID.class );
    }

    @Override protected AppConfigKey mapToKey( ResultSet rs ) {
        try {
            UUID appId = rs.getObject( APP_ID.getName(), UUID.class );
            UUID organizationId = rs.getObject( ORGANIZATION_ID.getName(), UUID.class );
            UUID appTypeId = rs.getObject( CONFIG_TYPE_ID.getName(), UUID.class );
            return new AppConfigKey( appId, organizationId, appTypeId );
        } catch ( SQLException ex ) {
            logger.error( "Unable to map row to AppConfigKey class", ex );
            return null;
        }
    }

    @Override public AppConfigKey generateTestKey() {
        return new AppConfigKey( UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID() );
    }

    @Override public UUID generateTestValue() {
        return UUID.randomUUID();
    }
}
