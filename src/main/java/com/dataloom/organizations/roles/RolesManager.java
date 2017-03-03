package com.dataloom.organizations.roles;

import java.util.UUID;

import com.dataloom.organization.roles.OrganizationRole;

public interface RolesManager {
    void createRoleIfNotExists( OrganizationRole role );
    
    void updateTitle( UUID organizationId, UUID roleId, String title );
    
    void updateDescription( UUID organizationId, UUID roleId, String description);
    
    Iterable<OrganizationRole> getAllRoles();
    
    void deleteRole( UUID organizationId, UUID roleId );
}
