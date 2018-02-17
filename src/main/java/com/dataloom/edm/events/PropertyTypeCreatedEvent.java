package com.dataloom.edm.events;

import com.openlattice.edm.type.PropertyType;

public class PropertyTypeCreatedEvent {
    
    private PropertyType propertyType;
    
    public PropertyTypeCreatedEvent( PropertyType propertyType ) {
        this.propertyType = propertyType;
    }
    
    public PropertyType getPropertyType() {
        return propertyType;
    }

}
