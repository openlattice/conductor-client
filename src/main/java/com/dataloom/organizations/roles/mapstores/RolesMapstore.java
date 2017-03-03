package com.dataloom.organizations.roles.mapstores;

import java.util.UUID;

import org.apache.commons.lang3.RandomStringUtils;

import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.organization.roles.OrganizationRole;
import com.dataloom.organizations.roles.RoleKey;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.base.Optional;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraMapstore;

public class RolesMapstore extends AbstractStructuredCassandraMapstore<RoleKey, OrganizationRole> {
    public RolesMapstore( Session session ) {
        super( HazelcastMap.ROLES.name(), session, Table.ROLES.getBuilder() );
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
    protected RoleKey mapKey( Row rs ) {
        return new RoleKey( rs.getUUID( CommonColumns.ORGANIZATION_ID.cql() ), rs.getUUID( CommonColumns.ID.cql() ) );
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

    @Override
    public RoleKey generateTestKey() {
        return new RoleKey( UUID.randomUUID(), UUID.randomUUID() );
    }

    @Override
    public OrganizationRole generateTestValue() {
        return new OrganizationRole(
                Optional.of( UUID.randomUUID() ),
                UUID.randomUUID(),
                RandomStringUtils.randomAlphanumeric( 5 ),
                Optional.of( RandomStringUtils.randomAlphanumeric( 5 ) ) );
    }

}
