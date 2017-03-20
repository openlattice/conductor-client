package com.dataloom.edm.events;

import java.util.UUID;

public class LinkingTypeDeletedEvent {
    
    private final UUID linkingTypeId;
    
    public LinkingTypeDeletedEvent( UUID linkingTypeId ) {
        this.linkingTypeId = linkingTypeId;
    }
    
    public UUID getLinkingTypeId() {
        return linkingTypeId;
    }

}
