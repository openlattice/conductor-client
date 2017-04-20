package com.dataloom.organizations.roles;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.authorization.Principal;
import com.dataloom.authorization.PrincipalType;
import com.dataloom.authorization.SystemRole;
import com.dataloom.edm.internal.DatastoreConstants;
import com.dataloom.organization.roles.OrganizationRole;

public class RolesUtil {
    private static final Logger logger = LoggerFactory.getLogger( RolesUtil.class );

    private RolesUtil() {}

    public static boolean belongsToOrganization( UUID organizationId, String roleKeyStr ) {
        if ( organizationId.equals( DatastoreConstants.DEFAULT_ORGANIZATION_ID ) ) {
            return SystemRole.contains( roleKeyStr );
        } else {
            return roleKeyStr.startsWith( organizationId.toString() );
        }
    }

    public static String getStringRepresentation( UUID organizationId, String title ) {
        if ( organizationId.equals( DatastoreConstants.DEFAULT_ORGANIZATION_ID ) ) {
            return title;
        } else {
            return organizationId + "|" + title;
        }
    }

    public static String getStringRepresentation( OrganizationRole role ) {
        return getStringRepresentation( role.getOrganizationId(), role.getTitle() );
    }

    /**
     * This method must be consistent with {@link #getStringRepresentation(UUID, String)}
     */
    public static UUID getOrganizationId( String stringRep ) {
        try {
            String[] splitted = stringRep.split( "\\|", 2 );
            return UUID.fromString( splitted[ 0 ] );
        } catch ( Exception e ) {
            if ( SystemRole.contains( stringRep ) ) {
                return DatastoreConstants.DEFAULT_ORGANIZATION_ID;
            } else {
                logger.error( "Error parsing organizationId from the string representation of role: " + stringRep );
                throw new IllegalArgumentException(
                        "Error parsing organizationId from the string representation of role: " + stringRep );
            }
        }
    }

    public static Principal getPrincipal( OrganizationRole role ) {
        return new Principal(
                PrincipalType.ROLE,
                getStringRepresentation( role.getOrganizationId(), role.getTitle() ) );
    }
}
