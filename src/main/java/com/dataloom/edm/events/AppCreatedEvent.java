package com.dataloom.edm.events;

import com.openlattice.apps.App;

public class AppCreatedEvent {

    private final App app;

    public AppCreatedEvent( App app ) {
        this.app = app;
    }

    public App getApp() {
        return app;
    }
}
