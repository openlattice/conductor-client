package com.dataloom.edm.events;

import com.openlattice.edm.type.AssociationType;

public class AssociationTypeCreatedEvent {

    private AssociationType associationType;

    public AssociationTypeCreatedEvent( AssociationType associationType ) {
        this.associationType = associationType;
    }

    public AssociationType getAssociationType() {
        return associationType;
    }

}
