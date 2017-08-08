package com.dataloom.organizations.roles;

import java.util.UUID;

import com.dataloom.authorization.Principal;
import com.dataloom.directory.pojo.Auth0UserBasic;
import com.dataloom.organization.roles.Role;
import com.dataloom.organization.roles.RoleKey;

public interface RolesManager {

    void createRoleIfNotExists( Principal principal, Role role );

    void updateTitle( RoleKey roleKey, String title );

    void updateDescription( RoleKey roleKey, String description );

    Role getRole( RoleKey roleKey );

    RoleKey getRoleKey( Principal principal );

    Iterable<Role> getAllRolesInOrganization( UUID organizationId );

    void deleteRole( RoleKey roleKey );

    void deleteAllRolesInOrganization( UUID organizationId, Iterable<Principal> users );

    // Methods about users

    void addRoleToUser( RoleKey roleKey, Principal user );

    void removeRoleFromUser( RoleKey roleKey, Principal user );

    Iterable<Principal> getAllUsersOfRole( RoleKey roleKey );

    Iterable<Auth0UserBasic> getAllUserProfilesOfRole( RoleKey roleKey );
}
