package com.dataloom.edm.types.processors;

import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import com.dataloom.edm.internal.EntityType;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;

public class AddPropertyTypesToEntityTypeProcessor extends AbstractRhizomeEntryProcessor<UUID, EntityType, Object> {
    private static final long serialVersionUID = -8192830001484436836L;
    private final Set<UUID>   propertyTypeIds;

    public AddPropertyTypesToEntityTypeProcessor( Set<UUID> propertyTypeIds ) {
        this.propertyTypeIds = propertyTypeIds;
    }

    @Override
    public Object process( Entry<UUID, EntityType> entry ) {
        EntityType et = entry.getValue();
        if ( et != null ) {
            et.addPropertyTypes( propertyTypeIds );
            entry.setValue( et );
        }
        return null;
    }

    public Set<UUID> getPropertyTypeIds() {
        return propertyTypeIds;
    }
}
