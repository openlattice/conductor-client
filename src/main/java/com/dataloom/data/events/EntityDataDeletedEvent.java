package com.dataloom.data.events;

import com.google.common.base.Optional;

import java.util.UUID;

public class EntityDataDeletedEvent {

    private final UUID           entitySetId;
    private final String         entityId;
    private final Optional<UUID> syncId;

    public EntityDataDeletedEvent( UUID entitySetId, String entityId, Optional<UUID> syncId ) {
        this.entitySetId = entitySetId;
        this.entityId = entityId;
        this.syncId = syncId;
    }

    public UUID getEntitySetId() {
        return entitySetId;
    }

    public String getEntityId() {
        return entityId;
    }

    public Optional<UUID> getSyncId() {
        return syncId;
    }

}
