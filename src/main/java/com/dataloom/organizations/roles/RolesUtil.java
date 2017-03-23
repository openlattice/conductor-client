package com.dataloom.organizations.roles;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.authorization.Principal;
import com.dataloom.authorization.PrincipalType;
import com.dataloom.organization.roles.RoleKey;
import com.google.common.base.Preconditions;

public class RolesUtil {
    private static final Logger logger = LoggerFactory.getLogger( RolesUtil.class );

    private RolesUtil() {}

    /**
     * Return RoleKey for a string of appropriate format, as specified in the toString method.
     * @param roleKeyStr
     * @return
     */
    public static RoleKey parse( String roleKeyStr ){
        UUID organizationId = null;
        UUID roleId = null;
        try {
            String[] parts = roleKeyStr.split( "|" );
            organizationId = UUID.fromString( parts[0] );
            roleId = UUID.fromString( parts[1] );
        } catch (Exception e ){
            logger.debug( "Error parsing to role key: " + roleKeyStr );
            throw new IllegalStateException( "Error parsing to role key: " + roleKeyStr );
        }
        return new RoleKey( organizationId, roleId );
    }
    
    public static RoleKey parse( Principal principal ){
        Preconditions.checkArgument( principal.getType() == PrincipalType.ROLE, "Principal " + principal + " is not of type ROLE.");
        return parse( principal.getId() );
    }
    
    public static boolean belongsToOrganization( UUID organizationId, String roleKeyStr ){
        return roleKeyStr.startsWith( organizationId.toString() );
    }

}
