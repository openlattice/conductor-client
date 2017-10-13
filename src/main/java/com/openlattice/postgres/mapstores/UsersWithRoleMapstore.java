package com.openlattice.postgres.mapstores;

import com.dataloom.authorization.Principal;
import com.dataloom.authorization.PrincipalType;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.mapstores.TestDataFactory;
import com.dataloom.organization.roles.RoleKey;
import com.dataloom.organizations.PrincipalSet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.openlattice.postgres.PostgresArrays;
import com.openlattice.postgres.PostgresColumnDefinition;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.openlattice.postgres.PostgresColumn.*;
import static com.openlattice.postgres.PostgresTable.ROLES;

public class UsersWithRoleMapstore extends AbstractBasePostgresMapstore<RoleKey, PrincipalSet> {

    public UsersWithRoleMapstore( HikariDataSource hds ) {
        super( HazelcastMap.USERS_WITH_ROLE.name(), ROLES, hds );
    }

    @Override protected List<PostgresColumnDefinition> keyColumns() {
        return ImmutableList.of( ROLE_ID, ORGANIZATION_ID );
    }

    @Override protected List<PostgresColumnDefinition> valueColumns() {
        return ImmutableList.of( PRINCIPAL_IDS );
    }

    @Override protected void bind( PreparedStatement ps, RoleKey key, PrincipalSet value ) throws SQLException {
        bind( ps, key );
        Array users = PostgresArrays.createTextArray(
                ps.getConnection(),
                value.stream().map( principal -> principal.getId() ) );
        ps.setArray( 3, users );
        ps.setArray( 4, users );
    }

    @Override protected void bind( PreparedStatement ps, RoleKey key ) throws SQLException {
        ps.setObject( 1, key.getRoleId() );
        ps.setObject( 2, key.getOrganizationId() );
    }

    @Override protected PrincipalSet mapToValue( ResultSet rs ) throws SQLException {
        Stream<String> users = Arrays.stream( (String[]) rs.getArray( PRINCIPAL_IDS.getName() ).getArray() );
        return PrincipalSet
                .wrap( users.map( user -> new Principal( PrincipalType.USER, user ) ).collect( Collectors.toSet() ) );
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
        return ImmutableList.of( ROLE_ID, ORGANIZATION_ID, PRINCIPAL_IDS );
    }

    @Override public RoleKey generateTestKey() {
        return TestDataFactory.roleKey();
    }

    @Override public PrincipalSet generateTestValue() {
        return PrincipalSet.wrap( ImmutableSet.of( TestDataFactory.userPrincipal(),
                TestDataFactory.userPrincipal(),
                TestDataFactory.userPrincipal() ) );
    }
}
