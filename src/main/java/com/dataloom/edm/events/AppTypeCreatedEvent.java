package com.dataloom.edm.events;

import com.dataloom.apps.AppType;

public class AppTypeCreatedEvent {

    private final AppType appType;

    public AppTypeCreatedEvent( AppType appType ) {
        this.appType = appType;
    }

    public AppType getAppType() {
        return appType;
    }
}
