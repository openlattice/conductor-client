package com.dataloom.edm.events;

import java.util.UUID;

public class EntityTypeDeletedEvent {
    
    private UUID entityTypeId;
    
    public EntityTypeDeletedEvent( UUID entityTypeId ) {
        this.entityTypeId = entityTypeId;
    }
    
    public UUID getEntityTypeId() {
        return entityTypeId;
    }

}
