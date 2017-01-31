package com.dataloom.edm.types.processors;

import java.util.Map.Entry;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.edm.internal.EntityType;
import com.google.common.base.Optional;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;

public class RenameEntityTypeProcessor extends AbstractRhizomeEntryProcessor<UUID, EntityType, Object> {
    private static final long       serialVersionUID = 6840767147392041814L;
    private final FullQualifiedName newFqn;

    public RenameEntityTypeProcessor( FullQualifiedName newFqn ) {
        this.newFqn = newFqn;
    }

    @Override
    public Object process( Entry<UUID, EntityType> entry ) {
        EntityType et = entry.getValue();
        if ( et != null ) {
            EntityType newEt = new EntityType(
                    et.getId(),
                    newFqn,
                    et.getTitle(),
                    Optional.of( et.getDescription() ),
                    et.getSchemas(),
                    et.getKey(),
                    et.getProperties() );
            entry.setValue( newEt );
        }
        return null;
    }
    
    public FullQualifiedName getFullQualifiedName(){
        return newFqn;
    }

}
