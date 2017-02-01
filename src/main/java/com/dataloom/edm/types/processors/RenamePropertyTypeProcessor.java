package com.dataloom.edm.types.processors;

import java.util.Map.Entry;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.edm.internal.PropertyType;
import com.google.common.base.Optional;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;

public class RenamePropertyTypeProcessor extends AbstractRhizomeEntryProcessor<UUID, PropertyType, Object> {
    private static final long       serialVersionUID = -7292989266784337619L;
    private final FullQualifiedName newFqn;

    public RenamePropertyTypeProcessor( FullQualifiedName newFqn ) {
        this.newFqn = newFqn;
    }

    @Override
    public Object process( Entry<UUID, PropertyType> entry ) {
        PropertyType pt = entry.getValue();
        if ( pt != null ) {
            PropertyType newPt = new PropertyType(
                    pt.getId(),
                    newFqn,
                    pt.getTitle(),
                    Optional.of( pt.getDescription() ),
                    pt.getSchemas(),
                    pt.getDatatype() );
            entry.setValue( newPt );
        }
        return null;
    }
    
    public FullQualifiedName getFullQualifiedName(){
        return newFqn;
    }

}