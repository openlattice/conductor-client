package com.dataloom.edm.types.processors;

import com.dataloom.edm.type.EntityType;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class AddPrimaryKeysToEntityTypeProcessor extends AbstractRhizomeEntryProcessor<UUID, EntityType, Object> {
    private static final long serialVersionUID = -3059625525089930061L;
    private final Set<UUID> propertyTypeIds;

    public AddPrimaryKeysToEntityTypeProcessor( Set<UUID> propertyTypeIds ) {
        this.propertyTypeIds = propertyTypeIds;
    }

    @Override
    public Object process( Map.Entry<UUID, EntityType> entry ) {
        EntityType et = entry.getValue();
        if ( et != null ) {
            et.addPrimaryKeys( propertyTypeIds );
            entry.setValue( et );
        }
        return null;
    }

    public Set<UUID> getPropertyTypeIds() {
        return propertyTypeIds;
    }
}
