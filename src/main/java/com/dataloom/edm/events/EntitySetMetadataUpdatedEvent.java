package com.dataloom.edm.events;

import com.dataloom.edm.internal.EntitySet;

public class EntitySetMetadataUpdatedEvent {
    
    private EntitySet entitySet;
    
    public EntitySetMetadataUpdatedEvent( EntitySet entitySet ) {
        this.entitySet = entitySet;
    }
    
    public EntitySet getEntitySet() {
        return entitySet;
    }

}
