package com.dataloom.organizations.roles;

import java.util.UUID;

import com.dataloom.authorization.Principal;
import com.dataloom.directory.pojo.Auth0UserBasic;
import com.dataloom.organization.roles.OrganizationRole;
import com.dataloom.organization.roles.RoleKey;

public interface RolesManager {
    void createRoleIfNotExists( OrganizationRole role );

    void updateTitle( RoleKey roleKey, String title );

    void updateDescription( RoleKey roleKey, String description );
    
    OrganizationRole getRole( RoleKey roleKey );

    Iterable<OrganizationRole> getAllRoles( UUID organizationId );

    void deleteRole( RoleKey roleKey );

    //Methods about users
    
    void addRoleToUser( RoleKey roleKey, Principal user );

    void removeRoleFromUser( RoleKey roleKey, Principal user );

    Iterable<Principal> getAllUsersOfRole( RoleKey roleKey );

    Iterable<Auth0UserBasic> getAllUserProfilesOfRole( RoleKey roleKey );
}
