package com.dataloom.sync.events;

import java.util.UUID;

public class LatestSyncUpdatedEvent {
    
    private final UUID entitySetId;
    private final UUID latestSyncId;
    
    public LatestSyncUpdatedEvent( UUID entitySetId, UUID latestSyncId ) {
        this.entitySetId = entitySetId;
        this.latestSyncId = latestSyncId;
    }
    
    public UUID getEntitySetId() {
        return entitySetId;
    }
    
    public UUID getLatestSyncId() {
        return latestSyncId;
    }

}
