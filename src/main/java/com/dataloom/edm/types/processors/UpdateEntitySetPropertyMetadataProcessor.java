package com.dataloom.edm.types.processors;

import java.util.Map.Entry;

import com.dataloom.edm.requests.MetadataUpdate;
import com.dataloom.edm.set.EntitySetPropertyKey;
import com.dataloom.edm.set.EntitySetPropertyMetadata;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;

public class UpdateEntitySetPropertyMetadataProcessor
        extends AbstractRhizomeEntryProcessor<EntitySetPropertyKey, EntitySetPropertyMetadata, Object> {
    private static final long    serialVersionUID = 8300328089856740121L;

    private final MetadataUpdate update;

    public UpdateEntitySetPropertyMetadataProcessor( MetadataUpdate update ) {
        this.update = update;
    }

    @Override
    public Object process( Entry<EntitySetPropertyKey, EntitySetPropertyMetadata> entry ) {
        EntitySetPropertyMetadata metadata = entry.getValue();
        if ( metadata != null ) {
            if ( update.getTitle().isPresent() ) {
                metadata.setTitle( update.getTitle().get() );
            }
            if ( update.getDescription().isPresent() ) {
                metadata.setDescription( update.getDescription().get() );
            }
            if ( update.getDefaultShow().isPresent() ) {
                metadata.setDefaultShow( update.getDefaultShow().get() );
            }
            entry.setValue( metadata );
        }
        return null;
    }
    
    public MetadataUpdate getUpdate() {
        return update;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( update == null ) ? 0 : update.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        UpdateEntitySetPropertyMetadataProcessor other = (UpdateEntitySetPropertyMetadataProcessor) obj;
        if ( update == null ) {
            if ( other.update != null ) return false;
        } else if ( !update.equals( other.update ) ) return false;
        return true;
    }

}
