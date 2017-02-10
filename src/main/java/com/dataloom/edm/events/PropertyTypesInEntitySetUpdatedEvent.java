package com.dataloom.edm.events;

import java.util.List;
import java.util.UUID;

import com.dataloom.edm.internal.PropertyType;

public class PropertyTypesInEntitySetUpdatedEvent {
    
    private UUID entitySetId;
    private List<PropertyType> newPropertyTypes;
    
    public PropertyTypesInEntitySetUpdatedEvent( UUID entitySetId, List<PropertyType> newPropertyTypes ) {
        this.entitySetId = entitySetId;
        this.newPropertyTypes = newPropertyTypes;
    }
    
    public UUID getEntitySetId() {
        return entitySetId;
    }
    
    public List<PropertyType> getNewPropertyTypes() {
        return newPropertyTypes;
    }

}
