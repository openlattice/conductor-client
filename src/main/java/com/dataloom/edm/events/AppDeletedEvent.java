package com.dataloom.edm.events;

import java.util.UUID;

public class AppDeletedEvent {

    private final UUID appId;

    public AppDeletedEvent( UUID appId ) {
        this.appId = appId;
    }

    public UUID getAppId() {
        return appId;
    }
}
