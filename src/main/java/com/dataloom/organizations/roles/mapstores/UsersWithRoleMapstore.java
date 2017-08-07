package com.dataloom.organizations.roles.mapstores;

import java.util.Set;
import java.util.stream.Collectors;

import com.dataloom.authorization.Principal;
import com.dataloom.authorization.PrincipalType;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.mapstores.TestDataFactory;
import com.dataloom.organization.roles.RoleKey;
import com.dataloom.organizations.PrincipalSet;
import com.dataloom.organizations.mapstores.UserSetMapstore;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.cassandra.RowAdapters;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraMapstore;

public class UsersWithRoleMapstore extends AbstractStructuredCassandraMapstore<RoleKey, PrincipalSet> {

    public UsersWithRoleMapstore( Session session ) {
        super(
                HazelcastMap.USERS_WITH_ROLE.name(),
                session,
                HazelcastMap.USERS_WITH_ROLE.getTable().getBuilder()
        );
    }

    @Override
    public RoleKey generateTestKey() {
        return TestDataFactory.roleKey();
    }

    @Override
    public PrincipalSet generateTestValue() {
        return UserSetMapstore.testValue();
    }

    @Override
    protected BoundStatement bind( RoleKey key, BoundStatement bs ) {
        return bs
                .setUUID( CommonColumns.ID.cql(), key.getRoleId() )
                .setUUID( CommonColumns.ORGANIZATION_ID.cql(), key.getOrganizationId() );
    }

    @Override
    protected BoundStatement bind( RoleKey key, PrincipalSet value, BoundStatement bs ) {
        return bind( key, bs ).setSet(
                CommonColumns.PRINCIPAL_IDS.cql(),
                value.stream().map( Principal::getId ).collect( Collectors.toSet() ),
                String.class
        );
    }

    @Override
    protected RoleKey mapKey( Row row ) {
        return RowAdapters.roleKey( row );
    }

    @Override
    protected PrincipalSet mapValue( ResultSet rs ) {
        Row r = rs.one();
        if ( r == null ) {
            return null;
        }
        Set<String> users = r.getSet( CommonColumns.PRINCIPAL_IDS.cql(), String.class );
        return PrincipalSet.wrap(
                users.stream().map( user -> new Principal( PrincipalType.USER, user ) ).collect( Collectors.toSet() ) );
    }

}
