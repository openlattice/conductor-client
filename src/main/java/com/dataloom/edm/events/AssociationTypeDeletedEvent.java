package com.dataloom.edm.events;

import java.util.UUID;

public class AssociationTypeDeletedEvent {

    private UUID associationTypeId;

    public AssociationTypeDeletedEvent( UUID associationTypeId ) {
        this.associationTypeId = associationTypeId;
    }

    public UUID getAssociationTypeId() {
        return associationTypeId;
    }

}
