package com.dataloom.edm.types.processors;

import java.util.LinkedHashSet;
import java.util.Map.Entry;
import java.util.UUID;

import com.dataloom.edm.type.EntityType;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;

public class ReorderPropertyTypesInEntityTypeProcessor extends AbstractRhizomeEntryProcessor<UUID, EntityType, Object> {
    private static final long         serialVersionUID = -6711199174498755908L;

    private final LinkedHashSet<UUID> propertyTypeIds;

    public ReorderPropertyTypesInEntityTypeProcessor( LinkedHashSet<UUID> propertyTypeIds ) {
        this.propertyTypeIds = propertyTypeIds;
    }

    @Override
    public Object process( Entry<UUID, EntityType> entry ) {
        EntityType et = entry.getValue();
        if ( et != null ) {
            et.reorderPropertyTypes( propertyTypeIds );
            entry.setValue( et );
        }
        return null;
    }

    public LinkedHashSet<UUID> getPropertyTypeIds() {
        return propertyTypeIds;
    }
}
