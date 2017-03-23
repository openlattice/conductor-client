package com.dataloom.organizations.roles;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.organization.roles.OrganizationRole;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.common.collect.Iterables;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.cassandra.RowAdapters;

public class RolesQueryService {
    private static final Logger                            logger = LoggerFactory
            .getLogger( RolesQueryService.class );
    private final Session                                  session;
    private final PreparedStatement                        getAllRolesInOrganizationQuery;
    private final PreparedStatement                        deleteAllRolesInOrganizationQuery;

    public RolesQueryService( Session session ) {
        this.session = session;
        
        this.getAllRolesInOrganizationQuery = session.prepare( Table.ORGANIZATIONS_ROLES.getBuilder().buildLoadByPartitionKeyQuery() );
        this.deleteAllRolesInOrganizationQuery = session.prepare( Table.ORGANIZATIONS_ROLES.getBuilder().buildDeleteByPartitionKeyQuery() );
    }
    
    public Iterable<OrganizationRole> getAllRolesInOrganization( UUID organizationId ){
        BoundStatement bs = getAllRolesInOrganizationQuery.bind().setUUID( CommonColumns.ORGANIZATION_ID.cql(), organizationId );
        return Iterables.transform( session.execute( bs ), RowAdapters::organizationRole );
    }
    
    public void deleteAllRolesInOrganization( UUID organizationId ){
        BoundStatement bs = deleteAllRolesInOrganizationQuery.bind().setUUID( CommonColumns.ORGANIZATION_ID.cql(), organizationId );
        session.execute( bs );
    }
}
