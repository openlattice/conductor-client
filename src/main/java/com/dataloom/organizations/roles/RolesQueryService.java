package com.dataloom.organizations.roles;

import com.dataloom.organization.roles.Role;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.openlattice.postgres.*;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

public class RolesQueryService {

    private static final Logger logger = LoggerFactory.getLogger( RolesQueryService.class );

    private final HikariDataSource hds;

    private final String getAllRolesInOrganizationSql;
    private final String deleteAllRolesInOrganizationSql;

    public RolesQueryService( HikariDataSource hds ) {
        this.hds = hds;

        // Table
        String ROLES = PostgresTable.ROLES.getName();

        // Columns
        String ROLE_ID = PostgresColumn.ROLE_ID.getName();
        String ORG_ID = PostgresColumn.ORGANIZATION_ID.getName();

        this.getAllRolesInOrganizationSql = PostgresQuery.selectFrom( ROLES ).concat( PostgresQuery.whereEq(
                ImmutableList.of( ORG_ID ), true ) );
        this.deleteAllRolesInOrganizationSql = PostgresQuery.deleteFrom( ROLES )
                .concat( PostgresQuery.whereEq( ImmutableList.of( ORG_ID ), true ) );

    }

    public List<Role> getAllRolesInOrganization( UUID organizationId ) {
        try {
            List<Role> result = Lists.newArrayList();
            Connection connection = hds.getConnection();
            PreparedStatement ps = connection.prepareStatement( getAllRolesInOrganizationSql );
            ps.setObject( 1, organizationId );

            ResultSet rs = ps.executeQuery();
            while ( rs.next() ) {
                result.add( ResultSetAdapters.role( rs ) );
            }
            connection.close();
            return result;
        } catch ( SQLException e ) {
            logger.debug( "Unable to get all roles for organization {}.", organizationId, e );
            return ImmutableList.of();
        }
    }

    public void deleteAllRolesInOrganization( UUID organizationId ) {
        try {
            Connection connection = hds.getConnection();
            PreparedStatement ps = connection.prepareStatement( getAllRolesInOrganizationSql );
            ps.setObject( 1, organizationId );
            ps.execute();
            connection.close();
            logger.info( "deleted all roles that belong to organizationId: {}", organizationId );
        } catch ( SQLException e ) {
            logger.debug( "Unable to delete all roles for organization {}.", organizationId, e );
        }
    }
}
