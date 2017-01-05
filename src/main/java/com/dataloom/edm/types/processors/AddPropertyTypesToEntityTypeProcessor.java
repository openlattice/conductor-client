package com.dataloom.edm.types.processors;

import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import com.dataloom.edm.internal.EntityType;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;

public class AddPropertyTypesToEntityTypeProcessor extends AbstractRhizomeEntryProcessor<UUID, EntityType, Object> {

    private final Set<UUID> propertyType;

    @Override
    public Object process( Entry<UUID, EntityType> entry ) {
        EntityType et = entry.getValue();
        if ( et != null ) {
            et.getProperties().addAll();
        }
        return null;
    }

}
