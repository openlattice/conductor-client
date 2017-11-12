package com.openlattice.postgres.mapstores;

import com.dataloom.apps.AppConfigKey;
import com.dataloom.apps.AppTypeSetting;
import com.dataloom.authorization.Permission;
import com.dataloom.hazelcast.HazelcastMap;
import com.google.common.collect.ImmutableList;
import com.openlattice.postgres.PostgresArrays;
import com.openlattice.postgres.PostgresColumnDefinition;
import com.openlattice.postgres.PostgresTable;
import com.openlattice.postgres.ResultSetAdapters;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.openlattice.postgres.PostgresColumn.*;

public class AppConfigMapstore extends AbstractBasePostgresMapstore<AppConfigKey, AppTypeSetting> {
    public AppConfigMapstore( HikariDataSource hds ) {
        super( HazelcastMap.APP_CONFIGS.name(), PostgresTable.APP_CONFIGS, hds );
    }

    @Override protected List<PostgresColumnDefinition> keyColumns() {
        return ImmutableList.of( APP_ID, ORGANIZATION_ID, CONFIG_TYPE_ID );
    }

    @Override protected List<PostgresColumnDefinition> valueColumns() {
        return ImmutableList.of( ENTITY_SET_ID, PERMISSIONS );
    }

    @Override protected void bind( PreparedStatement ps, AppConfigKey key, AppTypeSetting value ) throws SQLException {
        bind( ps, key );

        Array permissions = PostgresArrays.createTextArray( ps.getConnection(),
                value.getPermissions().stream().map( permission -> permission.toString() ) );

        ps.setObject( 4, value.getEntitySetId() );
        ps.setArray( 5, permissions );

        // UPDATE
        ps.setObject( 6, value.getEntitySetId() );
        ps.setArray( 7, permissions );
    }

    @Override protected void bind( PreparedStatement ps, AppConfigKey key ) throws SQLException {
        ps.setObject( 1, key.getAppId() );
        ps.setObject( 2, key.getOrganizationId() );
        ps.setObject( 3, key.getAppTypeId() );
    }

    @Override protected AppTypeSetting mapToValue( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.appTypeSetting( rs );
    }

    @Override protected AppConfigKey mapToKey( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.appConfigKey( rs );
    }

    @Override public AppConfigKey generateTestKey() {
        return new AppConfigKey( UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID() );
    }

    @Override public AppTypeSetting generateTestValue() {
        return new AppTypeSetting( UUID.randomUUID(), EnumSet.of( Permission.READ, Permission.WRITE ) );
    }
}
