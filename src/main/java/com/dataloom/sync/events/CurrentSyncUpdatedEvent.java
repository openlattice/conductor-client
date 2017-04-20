package com.dataloom.sync.events;

import java.util.UUID;

public class CurrentSyncUpdatedEvent {

    private final UUID entitySetId;
    private final UUID currentSyncId;

    public CurrentSyncUpdatedEvent( UUID entitySetId, UUID currentSyncId ) {
        this.entitySetId = entitySetId;
        this.currentSyncId = currentSyncId;
    }

    public UUID getEntitySetId() {
        return entitySetId;
    }

    public UUID getCurrentSyncId() {
        return currentSyncId;
    }

}
