package com.dataloom.edm.types.processors;

import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import com.openlattice.edm.type.AssociationType;
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

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( entityTypeIds == null ) ? 0 : entityTypeIds.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        RemoveSrcEntityTypesFromAssociationTypeProcessor other = (RemoveSrcEntityTypesFromAssociationTypeProcessor) obj;
        if ( entityTypeIds == null ) {
            if ( other.entityTypeIds != null ) return false;
        } else if ( !entityTypeIds.equals( other.entityTypeIds ) ) return false;
        return true;
    }

}
