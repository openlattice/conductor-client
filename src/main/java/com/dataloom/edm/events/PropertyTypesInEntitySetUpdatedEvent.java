package com.dataloom.edm.events;

import java.util.Set;
import java.util.UUID;

import com.dataloom.edm.internal.PropertyType;

public class PropertyTypesInEntitySetUpdatedEvent {
    
    private UUID entitySetId;
    private Set<PropertyType> newPropertyTypes;
    
    public PropertyTypesInEntitySetUpdatedEvent( UUID entitySetId, Set<PropertyType> newPropertyTypes ) {
        this.entitySetId = entitySetId;
        this.newPropertyTypes = newPropertyTypes;
    }
    
    public UUID getEntitySetId() {
        return entitySetId;
    }
    
    public Set<PropertyType> getNewPropertyTypes() {
        return newPropertyTypes;
    }

}
