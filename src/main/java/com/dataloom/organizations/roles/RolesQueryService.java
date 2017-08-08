package com.dataloom.organizations.roles;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.organization.roles.Role;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.cassandra.RowAdapters;

import static com.dataloom.streams.StreamUtil.stream;

public class RolesQueryService {

    private static final Logger logger = LoggerFactory.getLogger( RolesQueryService.class );

    private final Session           session;
    private final PreparedStatement getOrganizationIdQuery;
    private final PreparedStatement getAllRolesInOrganizationQuery;
    private final PreparedStatement deleteAllRolesInOrganizationQuery;

    public RolesQueryService( Session session ) {

        this.session = session;

        this.getAllRolesInOrganizationQuery = session.prepare(
                Table.ORGANIZATIONS_ROLES
                        .getBuilder()
                        .buildLoadByPartitionKeyQuery()
        );

        this.getOrganizationIdQuery = session.prepare(
                Table.ROLES
                        .getBuilder()
                        .buildLoadAllPrimaryKeysQuery()
                        .where( CommonColumns.ID.eq() )
        );

        this.deleteAllRolesInOrganizationQuery = session.prepare(
                Table.ROLES
                        .getBuilder()
                        .buildDeleteQuery()
                        .where( CommonColumns.ORGANIZATION_ID.eq() )
                        .and( QueryBuilder.in(
                                CommonColumns.ID.cql(),
                                CommonColumns.ID.bindMarker()
                        ) )
        );
    }

    public List<Role> getAllRolesInOrganization( UUID organizationId ) {

        BoundStatement bs = getAllRolesInOrganizationQuery.bind()
                .setUUID( CommonColumns.ORGANIZATION_ID.cql(), organizationId );

        return stream( session.execute( bs ) )
                .map( RowAdapters::role )
                .collect( Collectors.toList() );
    }

    public UUID getOrganizationId( UUID roleId ) {

        BoundStatement bs = getOrganizationIdQuery.bind()
                .setUUID( CommonColumns.ID.cql(), roleId );

        ResultSet resultSet = session.execute( bs );
        Row row = resultSet.one();
        return row == null ? null : RowAdapters.organizationId( row );
    }

    public void deleteAllRolesInOrganization( UUID organizationId, List<Role> allRolesInOrg ) {

        List<UUID> roleIds = allRolesInOrg
                .stream()
                .map( Role::getId )
                .collect( Collectors.toList() );

        BoundStatement bs = deleteAllRolesInOrganizationQuery.bind()
                .setUUID( CommonColumns.ORGANIZATION_ID.cql(), organizationId )
                .setList( CommonColumns.ID.cql(), roleIds, UUID.class );

        session.execute( bs );
        logger.info( "deleted all roles that belong to organizationId: {}", organizationId);
    }
}
