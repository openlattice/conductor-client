package com.dataloom.edm.types.processors;

import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import com.dataloom.edm.type.AssociationType;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;

public class AddSrcEntityTypesToAssociationTypeProcessor
        extends AbstractRhizomeEntryProcessor<UUID, AssociationType, Object> {
    private static final long serialVersionUID = -5486173538457874824L;

    private final Set<UUID>   entityTypeIds;

    public AddSrcEntityTypesToAssociationTypeProcessor( Set<UUID> entityTypeIds ) {
        this.entityTypeIds = entityTypeIds;
    }

    @Override
    public Object process( Entry<UUID, AssociationType> entry ) {
        AssociationType at = entry.getValue();
        if ( at != null ) {
            at.addSrcEntityTypes( entityTypeIds );
            entry.setValue( at );
        }
        return null;
    }

    public Set<UUID> getEntityTypeIds() {
        return entityTypeIds;
    }

}
