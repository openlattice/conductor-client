package com.dataloom.organizations.events;

import com.dataloom.authorization.Principal;
import com.dataloom.organization.Organization;

public class OrganizationCreatedEvent {
    
    private Organization organization;
    private Principal principal;
    
    public OrganizationCreatedEvent( Organization organization, Principal principal ) {
        this.organization = organization;
        this.principal = principal;
    }
    
    public Organization getOrganization() {
        return organization;
    }
    
    public Principal getPrincipal() {
        return principal;
    }

}
