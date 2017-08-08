package com.dataloom.organizations.roles.mapstores;

import java.util.UUID;

import org.apache.commons.lang3.RandomStringUtils;

import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.organization.roles.Role;
import com.dataloom.organization.roles.RoleKey;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.base.Optional;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.cassandra.RowAdapters;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraMapstore;

public class RolesMapstore extends AbstractStructuredCassandraMapstore<RoleKey, Role> {

    private Role testValue = generateRole();

    public RolesMapstore( Session session ) {
        super( HazelcastMap.ROLES.name(), session, Table.ROLES.getBuilder() );
    }

    @Override
    public RoleKey generateTestKey() {
        return testValue.getRoleKey();
    }

    @Override
    public Role generateTestValue() {
        return testValue;
    }

    @Override
    protected BoundStatement bind( RoleKey key, BoundStatement bs ) {
        return bs
                .setUUID( CommonColumns.ID.cql(), key.getRoleId() )
                .setUUID( CommonColumns.ORGANIZATION_ID.cql(), key.getOrganizationId() );
    }

    @Override
    protected BoundStatement bind( RoleKey key, Role value, BoundStatement bs ) {
        return bind( key, bs )
                .setString( CommonColumns.TITLE.cql(), value.getTitle() )
                .setString( CommonColumns.DESCRIPTION.cql(), value.getDescription() );
    }

    @Override
    protected RoleKey mapKey( Row row ) {
        return RowAdapters.roleKey( row );
    }

    @Override
    protected Role mapValue( ResultSet rs ) {
        Row row = rs.one();
        if ( row == null ) {
            return null;
        }
        Optional<UUID> id = Optional.of( row.getUUID( CommonColumns.ID.cql() ) );
        UUID organizationId = row.getUUID( CommonColumns.ORGANIZATION_ID.cql() );
        String title = row.getString( CommonColumns.TITLE.cql() );
        Optional<String> description = Optional.of( row.getString( CommonColumns.DESCRIPTION.cql() ) );
        return new Role( id, organizationId, title, description );
    }

    private Role generateRole(){
        return new Role(
                Optional.of( UUID.randomUUID() ),
                UUID.randomUUID(),
                RandomStringUtils.randomAlphanumeric( 5 ),
                Optional.of( RandomStringUtils.randomAlphanumeric( 5 ) )
        );
    }
}
