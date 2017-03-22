package com.dataloom.edm.types.processors;

import java.util.Map.Entry;
import java.util.UUID;

import com.dataloom.edm.EntitySet;
import com.dataloom.edm.requests.MetadataUpdate;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;

public class UpdateEntitySetMetadataProcessor extends AbstractRhizomeEntryProcessor<UUID, EntitySet, Object> {
    private static final long    serialVersionUID = 5385727595860961157L;
    private final MetadataUpdate update;

    public UpdateEntitySetMetadataProcessor( MetadataUpdate update ) {
        this.update = update;
    }

    @Override
    public Object process( Entry<UUID, EntitySet> entry ) {
        EntitySet es = entry.getValue();
        if ( es != null ) {
            if ( update.getTitle().isPresent() ) {
                es.setTitle( update.getTitle().get() );
            }
            if ( update.getDescription().isPresent() ) {
                es.setDescription( update.getDescription().get() );
            }
            if ( update.getName().isPresent() ) {
                es.setName( update.getName().get() );
            }
            if ( update.getContacts().isPresent() ) {
                es.setContacts( update.getContacts().get() );
            }
            entry.setValue( es );
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
        UpdateEntitySetMetadataProcessor other = (UpdateEntitySetMetadataProcessor) obj;
        if ( update == null ) {
            if ( other.update != null ) return false;
        } else if ( !update.equals( other.update ) ) return false;
        return true;
    }

}
