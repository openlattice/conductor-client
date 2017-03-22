package com.dataloom.edm.events;

import java.util.UUID;

public class PropertyTypeDeletedEvent {
    
    private UUID propertyTypeId;
    
    public PropertyTypeDeletedEvent( UUID propertyTypeId ) { 
        this.propertyTypeId = propertyTypeId;
    }
    
    public UUID getPropertyTypeId() {
        return propertyTypeId;
    }

}
