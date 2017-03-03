package com.dataloom.organizations.roles;

import java.util.UUID;

public class RoleKey {
    private UUID organizationId;
    private UUID roleId;

    public RoleKey( UUID organizationId, UUID roleId ) {
        this.organizationId = organizationId;
        this.roleId = roleId;
    }

    public UUID getOrganizationId() {
        return organizationId;
    }

    public UUID getRoleId() {
        return roleId;
    }

    @Override
    public String toString() {
        return "RoleKey [organizationId=" + organizationId + ", roleId=" + roleId + "]";
    }

}
