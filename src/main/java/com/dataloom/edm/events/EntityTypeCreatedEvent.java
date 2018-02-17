package com.dataloom.edm.events;

import com.openlattice.edm.type.EntityType;

public class EntityTypeCreatedEvent {
    
    private EntityType entityType;
    
    public EntityTypeCreatedEvent( EntityType entityType ) {
        this.entityType = entityType;
    }
    
    public EntityType getEntityType() {
        return entityType;
    }

}
