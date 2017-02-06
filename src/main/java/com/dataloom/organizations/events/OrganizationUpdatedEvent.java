package com.dataloom.organizations.events;

import java.util.UUID;

import com.google.common.base.Optional;

public class OrganizationUpdatedEvent {
    
    private UUID id;
    private Optional<String> optionalTitle;
    private Optional<String> optionalDescription;
    
    public OrganizationUpdatedEvent( UUID id, Optional<String> optionalTitle, Optional<String> optionalDescription ) {
        this.id = id;
        this.optionalTitle = optionalTitle;
        this.optionalDescription = optionalDescription;
    }
    
    public UUID getId() {
        return id;
    }
    
    public Optional<String> getOptionalTitle() {
        return optionalTitle;
    }
    
    public Optional<String> getOptionalDescription() {
        return optionalDescription;
    }

}
