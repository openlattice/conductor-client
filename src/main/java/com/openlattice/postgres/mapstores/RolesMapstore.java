package com.openlattice.postgres.mapstores;

import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.mapstores.TestDataFactory;
import com.dataloom.organization.roles.Role;
import com.dataloom.organization.roles.RoleKey;
import com.google.common.collect.ImmutableList;
import com.openlattice.postgres.PostgresColumnDefinition;
import com.openlattice.postgres.ResultSetAdapters;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static com.openlattice.postgres.PostgresColumn.*;
import static com.openlattice.postgres.PostgresTable.ROLES;

public class RolesMapstore extends AbstractBasePostgresMapstore<RoleKey, Role> {

    public RolesMapstore( HikariDataSource hds ) {
        super( HazelcastMap.ROLES.name(), ROLES, hds );
    }

    @Override protected List<PostgresColumnDefinition> keyColumns() {
        return ImmutableList.of( ROLE_ID, ORGANIZATION_ID );
    }

    @Override protected List<PostgresColumnDefinition> valueColumns() {
        return ImmutableList.of( NULLABLE_TITLE, DESCRIPTION );
    }

    @Override protected void bind( PreparedStatement ps, RoleKey key, Role value ) throws SQLException {
        bind( ps, key );
        ps.setString( 3, value.getTitle() );
        ps.setString( 4, value.getDescription() );
        ps.setString( 5, value.getTitle() );
        ps.setString( 6, value.getDescription() );
    }

    @Override protected void bind( PreparedStatement ps, RoleKey key ) throws SQLException {
        ps.setObject( 1, key.getRoleId() );
        ps.setObject( 2, key.getOrganizationId() );
    }

    @Override protected Role mapToValue( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.role( rs );
    }

    @Override protected RoleKey mapToKey( ResultSet rs ) {
        try {
            return ResultSetAdapters.roleKey( rs );
        } catch ( SQLException ex ) {
            logger.error( "Unable to map row to RoleKey", ex );
            return null;
        }
    }

    @Override
    protected List<PostgresColumnDefinition> getInsertColumns() {
        return ImmutableList.of( ROLE_ID, ORGANIZATION_ID, NULLABLE_TITLE, DESCRIPTION );
    }

    @Override public RoleKey generateTestKey() {
        return TestDataFactory.roleKey();
    }

    @Override public Role generateTestValue() {
        return TestDataFactory.role();
    }
}
