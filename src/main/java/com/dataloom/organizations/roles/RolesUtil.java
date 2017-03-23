package com.dataloom.organizations.roles;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RolesUtil {
    private static final Logger logger = LoggerFactory.getLogger( RolesUtil.class );

    private RolesUtil() {}

    public static boolean belongsToOrganization( UUID organizationId, String roleKeyStr ) {
        return roleKeyStr.startsWith( organizationId.toString() );
    }

}
