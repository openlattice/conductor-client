package com.dataloom.sync.events;

import java.util.List;
import java.util.UUID;

import com.dataloom.edm.type.PropertyType;

public class SyncIdCreatedEvent {
    
    private final UUID entitySetId;
    private final UUID syncId;
    private List<PropertyType> propertyTypes;
    
    public SyncIdCreatedEvent( UUID entitySetId, UUID syncId, List<PropertyType> propertyTypes ) {
        this.entitySetId = entitySetId;
        this.syncId = syncId;
        this.propertyTypes = propertyTypes;
    }
    
    public UUID getEntitySetId() {
        return entitySetId;
    }
    
    public UUID getSyncId() {
        return syncId;
    }
    
    public List<PropertyType> getPropertyTypes() {
        return propertyTypes;
    }

}
