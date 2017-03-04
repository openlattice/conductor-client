package com.dataloom.organizations.roles;

import java.util.Set;
import java.util.UUID;

import com.dataloom.authorization.Principal;
import com.dataloom.organization.roles.OrganizationRole;

public interface RolesManager {
    void createRoleIfNotExists( OrganizationRole role );

    void updateTitle( String roleIdString, String title );

    void updateDescription( String roleIdString, String description );
    
    OrganizationRole getRole( String roleIdString );

    Iterable<OrganizationRole> getAllRoles();

    void deleteRole( String roleIdString );

    //Methods about users
    
    void addRoleToUser( String userId, String roleIdString );

    void removeRoleFromUser( String userId, String roleIdString );

    Set<Principal> getAllUsersOfRole( String roleIdString );
}
