package com.dataloom.organizations.roles.mapstores;

import java.util.UUID;

import org.apache.commons.lang3.RandomStringUtils;

import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.mapstores.TestDataFactory;
import com.dataloom.organization.roles.OrganizationRole;
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

public class RolesMapstore extends AbstractStructuredCassandraMapstore<RoleKey, OrganizationRole> {

    private OrganizationRole testValue = generateOrganizationRole();
    
    public RolesMapstore( Session session ) {
        super( HazelcastMap.ORGANIZATIONS_ROLES.name(), session, Table.ORGANIZATIONS_ROLES.getBuilder() );
    }

    @Override
    public RoleKey generateTestKey() {
        return testValue.getRoleKey();
    }

    @Override
    public OrganizationRole generateTestValue() {
        return testValue;
    }

    @Override
    protected BoundStatement bind( RoleKey key, BoundStatement bs ) {
        return bs.setUUID( CommonColumns.ORGANIZATION_ID.cql(), key.getOrganizationId() )
                .setUUID( CommonColumns.ID.cql(), key.getRoleId() );
    }

    @Override
    protected BoundStatement bind( RoleKey key, OrganizationRole value, BoundStatement bs ) {
        return bind( key, bs ).setString( CommonColumns.TITLE.cql(), value.getTitle() )
                .setString( CommonColumns.DESCRIPTION.cql(), value.getDescription() );
    }

    @Override
    protected RoleKey mapKey( Row row ) {
        return RowAdapters.roleKey( row );
    }

    @Override
    protected OrganizationRole mapValue( ResultSet rs ) {
        Row row = rs.one();
        if ( row == null ) {
            return null;
        }
        Optional<UUID> id = Optional.of( row.getUUID( CommonColumns.ID.cql() ) );
        UUID organizationId = row.getUUID( CommonColumns.ORGANIZATION_ID.cql() );
        String title = row.getString( CommonColumns.TITLE.cql() );
        Optional<String> description = Optional.of( row.getString( CommonColumns.DESCRIPTION.cql() ) );
        return new OrganizationRole( id, organizationId, title, description );
    }

    private OrganizationRole generateOrganizationRole(){
        return new OrganizationRole(
                Optional.of( UUID.randomUUID() ),
                UUID.randomUUID(),
                RandomStringUtils.randomAlphanumeric( 5 ),
                Optional.of( RandomStringUtils.randomAlphanumeric( 5 ) ) );
    }
}
