package com.dataloom.edm.types.processors;

import com.openlattice.edm.type.EntityType;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class RemovePrimaryKeysFromEntityTypeProcessor extends AbstractRhizomeEntryProcessor<UUID, EntityType, Object> {
    private static final long serialVersionUID = -1052134829487106527L;
    private final Set<UUID> propertyTypeIds;

    public RemovePrimaryKeysFromEntityTypeProcessor( Set<UUID> propertyTypeIds ) {
        this.propertyTypeIds = propertyTypeIds;
    }

    @Override
    public Object process( Map.Entry<UUID, EntityType> entry ) {
        EntityType et = entry.getValue();
        if ( et != null ) {
            et.removePrimaryKeys( propertyTypeIds );
            entry.setValue( et );
        }
        return null;
    }

    public Set<UUID> getPropertyTypeIds() {
        return propertyTypeIds;
    }

}
