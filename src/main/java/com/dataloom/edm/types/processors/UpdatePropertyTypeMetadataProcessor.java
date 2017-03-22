package com.dataloom.edm.types.processors;

import java.util.Map.Entry;
import java.util.UUID;

import com.dataloom.edm.requests.MetadataUpdate;
import com.dataloom.edm.type.PropertyType;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;

public class UpdatePropertyTypeMetadataProcessor extends AbstractRhizomeEntryProcessor<UUID, PropertyType, Object> {
    private static final long serialVersionUID = -1970049507051915211L;
    private final MetadataUpdate update;

    public UpdatePropertyTypeMetadataProcessor( MetadataUpdate update ) {
        this.update = update;
    }

    @Override
    public Object process( Entry<UUID, PropertyType> entry ) {
        PropertyType pt = entry.getValue();
        if ( pt != null ) {
            if( update.getTitle().isPresent() ){
                pt.setTitle( update.getTitle().get() );
            }
            if( update.getDescription().isPresent() ){
                pt.setDescription( update.getDescription().get() );
            }
            if( update.getType().isPresent() ){
                pt.setType( update.getType().get() );
            }
            entry.setValue( pt );
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
        UpdatePropertyTypeMetadataProcessor other = (UpdatePropertyTypeMetadataProcessor) obj;
        if ( update == null ) {
            if ( other.update != null ) return false;
        } else if ( !update.equals( other.update ) ) return false;
        return true;
    }

}
