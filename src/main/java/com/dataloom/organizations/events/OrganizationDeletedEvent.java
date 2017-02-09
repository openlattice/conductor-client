package com.dataloom.organizations.events;

import java.util.UUID;

public class OrganizationDeletedEvent {
    
    private UUID organizationId;
    
    public OrganizationDeletedEvent( UUID organizationId ) {
        this.organizationId = organizationId;
    }
    
    public UUID getOrganizationId() {
        return organizationId;
    }

}
