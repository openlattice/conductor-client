package com.dataloom.edm.types.processors;

import java.util.Map.Entry;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import java.util.UUID;

import com.dataloom.edm.internal.EntitySet;
import com.google.common.base.Optional;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;

public class RenameEntitySetProcessor extends AbstractRhizomeEntryProcessor<UUID, EntitySet, Object> {
    private static final long serialVersionUID = 2745124176656423898L;
    private final String      newName;

    public RenameEntitySetProcessor( String newName ) {
        this.newName = newName;
    }

    @Override
    public Object process( Entry<UUID, EntitySet> entry ) {
        EntitySet es = entry.getValue();
        if ( es != null ) {
            EntitySet newEs = new EntitySet(
                    es.getId(),
                    es.getEntityTypeId(),
                    newName,
                    es.getTitle(),
                    Optional.of( es.getDescription() ) );
            entry.setValue( newEs );
        }
        return null;
    }
    
    public String getName(){
        return newName;
    }

}