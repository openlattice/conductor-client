package com.dataloom.data.events;

import java.util.Map;
import java.util.UUID;

public class EntityDataCreatedEvent {
    
    private UUID entitySetId;
    private String entityId;
    private Map<UUID, String> propertyValues;
    
    public EntityDataCreatedEvent( UUID entitySetId, String entityId, Map<UUID, String> propertyValues ) {
        this.entitySetId = entitySetId;
        this.entityId = entityId;
        this.propertyValues = propertyValues;
    }
    
    public UUID getEntitySetId() {
        return entitySetId;
    }
    
    public String getEntityId() {
        return entityId;
    }
    
    public Map<UUID, String> getPropertyValues() {
        return propertyValues;
    }

}
