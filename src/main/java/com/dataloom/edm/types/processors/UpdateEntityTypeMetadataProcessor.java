package com.dataloom.edm.types.processors;

import java.util.Map.Entry;
import java.util.UUID;

import com.dataloom.edm.requests.MetadataUpdate;
import com.dataloom.edm.type.EntityType;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class UpdateEntityTypeMetadataProcessor extends AbstractRhizomeEntryProcessor<UUID, EntityType, Object> {
    private static final long    serialVersionUID = 5283397691478851914L;
    @SuppressFBWarnings(value = "SE_BAD_FIELD", justification = "Custom Stream Serializer is implemented")
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
        UpdateEntityTypeMetadataProcessor other = (UpdateEntityTypeMetadataProcessor) obj;
        if ( update == null ) {
            if ( other.update != null ) return false;
        } else if ( !update.equals( other.update ) ) return false;
        return true;
    }

}
