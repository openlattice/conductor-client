package com.dataloom.edm.types.processors;

import java.util.Map.Entry;
import java.util.UUID;

import com.dataloom.edm.requests.MetadataUpdate;
import com.dataloom.edm.type.EntityType;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;

public class UpdateEntityTypeMetadataProcessor extends AbstractRhizomeEntryProcessor<UUID, EntityType, Object> {
    private static final long    serialVersionUID = 5283397691478851914L;
    private final MetadataUpdate update;

    public UpdateEntityTypeMetadataProcessor( MetadataUpdate update ) {
        this.update = update;
    }

    @Override
    public Object process( Entry<UUID, EntityType> entry ) {
        EntityType et = entry.getValue();
        if ( et != null ) {
            if( update.getTitle().isPresent() ){
                et.setTitle( update.getTitle().get() );
            }
            if( update.getDescription().isPresent() ){
                et.setDescription( update.getDescription().get() );
            }
            if( update.getType().isPresent() ){
                et.setType( update.getType().get() );
            }
            entry.setValue( et );
        }
        return null;
    }

    public MetadataUpdate getUpdate() {
        return update;
    }

}
