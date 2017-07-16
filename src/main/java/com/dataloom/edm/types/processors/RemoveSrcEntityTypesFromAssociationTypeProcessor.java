package com.dataloom.edm.types.processors;

import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import com.dataloom.edm.type.AssociationType;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;

public class RemoveSrcEntityTypesFromAssociationTypeProcessor
        extends AbstractRhizomeEntryProcessor<UUID, AssociationType, Object> {

    private static final long serialVersionUID = 6609704696075232114L;

    private final Set<UUID>   entityTypeIds;

    public RemoveSrcEntityTypesFromAssociationTypeProcessor( Set<UUID> entityTypeIds ) {
        this.entityTypeIds = entityTypeIds;
    }

    @Override
    public Object process( Entry<UUID, AssociationType> entry ) {
        AssociationType at = entry.getValue();
        if ( at != null ) {
            at.removeSrcEntityTypes( entityTypeIds );
            entry.setValue( at );
        }
        return null;
    }

    public Set<UUID> getEntityTypeIds() {
        return entityTypeIds;
    }

}
