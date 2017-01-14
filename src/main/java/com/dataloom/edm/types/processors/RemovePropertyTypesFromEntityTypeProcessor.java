package com.dataloom.edm.types.processors;

import java.util.Map.Entry;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.dataloom.edm.internal.EntityType;
import com.google.common.base.Optional;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;

public class RemovePropertyTypesFromEntityTypeProcessor extends AbstractRhizomeEntryProcessor<UUID, EntityType, Object> {
    private static final long serialVersionUID = -8192830001484436836L;
    private final Set<UUID> propertyTypeIds ;
    
    public RemovePropertyTypesFromEntityTypeProcessor(Set<UUID> propertyTypeIds ) {
        this.propertyTypeIds = propertyTypeIds;
    }

    @Override
    public Object process( Entry<UUID, EntityType> entry ) {
        EntityType et = entry.getValue();
        if ( et != null ) {
            et.removePropertyTypes( propertyTypeIds );
            entry.setValue( et );
        }
        return null;
    }

}
