package com.dataloom.organizations.roles.mapstores;

import java.util.UUID;

import org.apache.commons.lang3.RandomStringUtils;

import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.organization.roles.OrganizationRole;
import com.dataloom.organizations.mapstores.UUIDKeyMapstore;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.base.Optional;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CommonColumns;

public class RolesMapstore extends UUIDKeyMapstore<OrganizationRole> {
    public RolesMapstore( Session session ) {
        super( HazelcastMap.ROLES, session, Table.ORGANIZATIONS_ROLES, CommonColumns.ID );
    }

    @Override
    protected BoundStatement bind( UUID key, OrganizationRole value, BoundStatement bs ) {
        return bind( key, bs ).setString( CommonColumns.TITLE.cql(), value.getTitle() )
                .setString( CommonColumns.DESCRIPTION.cql(), value.getDescription() );
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
    public OrganizationRole generateTestValue() {
        return new OrganizationRole(
                Optional.of( UUID.randomUUID() ),
                UUID.randomUUID(),
                RandomStringUtils.randomAlphanumeric( 5 ),
                Optional.of( RandomStringUtils.randomAlphanumeric( 5 ) ) );
    }

}
