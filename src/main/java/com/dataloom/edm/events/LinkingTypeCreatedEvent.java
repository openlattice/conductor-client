package com.dataloom.edm.events;

import com.dataloom.edm.type.LinkingType;

public class LinkingTypeCreatedEvent {

    private final LinkingType linkingType;

    public LinkingTypeCreatedEvent( LinkingType linkingType ) {
        this.linkingType = linkingType;
    }

    public LinkingType getLinkingType() {
        return linkingType;
    }

}
