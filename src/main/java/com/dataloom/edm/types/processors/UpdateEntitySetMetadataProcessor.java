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

}
