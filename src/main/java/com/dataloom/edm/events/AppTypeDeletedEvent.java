package com.dataloom.edm.events;

import java.util.UUID;

public class AppTypeDeletedEvent {

    private UUID appTypeId;

    public AppTypeDeletedEvent( UUID appTypeId ) {
        this.appTypeId = appTypeId;
    }

    public UUID getAppTypeId() {
        return appTypeId;
    }
}
