package com.openlattice.postgres.mapstores;

import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.mapstores.TestDataFactory;
import com.dataloom.organization.roles.Role;
import com.dataloom.organization.roles.RoleKey;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.openlattice.postgres.PostgresColumnDefinition;
import com.openlattice.postgres.PostgresTableDefinition;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

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
        UUID roleId = rs.getObject( ROLE_ID.getName(), UUID.class );
        UUID orgId = rs.getObject( ORGANIZATION_ID.getName(), UUID.class );
        String title = rs.getString( NULLABLE_TITLE.getName() );
        String description = rs.getString( DESCRIPTION.getName() );
        return new Role( Optional.of( roleId ), orgId, title, Optional.fromNullable( description ) );
    }

    @Override protected RoleKey mapToKey( ResultSet rs ) {
        try {
            UUID roleId = rs.getObject( ROLE_ID.getName(), UUID.class );
            UUID orgId = rs.getObject( ORGANIZATION_ID.getName(), UUID.class );
            return new RoleKey( orgId, roleId );
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
