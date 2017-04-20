package com.dataloom.sync.events;

import java.util.UUID;

public class SyncIdCreatedEvent {

    private final UUID entitySetId;
    private final UUID syncId;

    public SyncIdCreatedEvent( UUID entitySetId, UUID syncId ) {
        this.entitySetId = entitySetId;
        this.syncId = syncId;
    }

    public UUID getEntitySetId() {
        return entitySetId;
    }

    public UUID getSyncId() {
        return syncId;
    }

}
